package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config 是引擎运行参数。
type Config struct {
	// MaxConcurrency 同时运行的作业上限。<=0 时取默认值 4。
	MaxConcurrency int
	// DefaultRetry 作业未单独指定重试策略时使用的默认策略。
	DefaultRetry RetryPolicy
	// Logger 用于打印每次调度的关键输入与判定依据；nil 时用 slog.Default()。
	Logger *slog.Logger
}

// Engine 负责任务编排：DAG 调度、并发控制、重试、取消传播与状态持久化。
type Engine struct {
	store  Store
	cfg    Config
	logger *slog.Logger

	runMu sync.Mutex
	// running 保证同一批次不会被两个 Run 并发编排（重复提交串行化）。
	running map[BatchID]bool
}

// NewEngine 创建一个以 store 持久化状态的编排引擎。
func NewEngine(store Store, cfg Config) *Engine {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 4
	}
	cfg.DefaultRetry = normalizeRetry(cfg.DefaultRetry)
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		store:   store,
		cfg:     cfg,
		logger:  logger,
		running: make(map[BatchID]bool),
	}
}

// Run 提交并执行一批作业。
//
// 语义：
//   - 启动前先做 DAG 校验，发现循环依赖/未知依赖等问题直接拒绝执行；
//   - 以 batchID 作为幂等键：已 succeeded 的作业直接复用、不再执行；
//   - 上次残留的 running 状态（进程中断）会先被重置为 pending 再重新调度；
//   - 相同 batchID 的重复提交是安全的，不会重复产生已完成作业的副作用；
//   - ctx 取消或整体超时时，取消信号传播到每个正在执行的作业。
func (e *Engine) Run(ctx context.Context, batchID BatchID, jobs []Job) (*BatchResult, error) {
	if batchID == "" {
		return nil, errors.New("batchID 不能为空")
	}

	// 同一批次的重复提交必须串行编排，避免两个调度器同时执行同一作业。
	e.runMu.Lock()
	if e.running[batchID] {
		e.runMu.Unlock()
		return nil, fmt.Errorf("批次 %q 正在执行中，重复提交被拒绝", batchID)
	}
	e.running[batchID] = true
	defer func() {
		e.runMu.Lock()
		delete(e.running, batchID)
		e.runMu.Unlock()
	}()
	e.runMu.Unlock()

	log := e.logger.With("batch_id", batchID)

	// 1) 启动阶段校验：任何结构问题都在执行任何作业之前拒绝。
	if errs := ValidateDAG(jobs); len(errs) > 0 {
		for _, ve := range errs {
			log.Error("DAG 校验失败，拒绝启动",
				"kind", ve.Kind, "job", ve.JobID, "dep", ve.DepID, "reason", ve.Message)
		}
		return nil, fmt.Errorf("批次 %q DAG 校验失败: %s", batchID, joinValidationErrs(errs))
	}
	for _, j := range jobs {
		if j.Fn == nil {
			return nil, fmt.Errorf("作业 %q 缺少执行函数 Fn", j.ID)
		}
	}

	defs := make(map[JobID]Job, len(jobs))
	for _, j := range jobs {
		defs[j.ID] = j
	}

	// 2) 载入历史状态（首次提交 / 中断重启 / 重复提交 走同一条恢复路径）。
	state, err := e.store.Load(ctx, batchID)
	if errors.Is(err, ErrStateNotFound) {
		state = &BatchState{BatchID: batchID, Jobs: make(map[JobID]*JobState)}
	} else if err != nil {
		return nil, err
	}
	state.BatchID = batchID
	if state.Jobs == nil {
		state.Jobs = make(map[JobID]*JobState)
	}

	resumed := e.recoverState(state, defs, log)

	var mu sync.Mutex // 保护 state 与持久化
	var persist func(reason string) error
	persist = func(reason string) error {
		// 序列化必须与所有状态写入互斥：先在锁内完成“快照 + 版本推进”，
		// 再在锁外做磁盘 IO，避免 JSON 编码与 worker 写入发生数据竞争。
		mu.Lock()
		snap := cloneStateLocked(state)
		snap.Version++
		snap.UpdatedAt = time.Now().UTC()
		state.Version = snap.Version
		state.UpdatedAt = snap.UpdatedAt
		mu.Unlock()

		// 持久化不依赖批次 ctx：即便批次已取消，终态也要落盘，
		// 否则会留下状态不明确的中间态记录。
		if err := e.store.Save(context.Background(), snap); err != nil {
			log.Error("状态持久化失败", "reason", reason, "err", err)
			return err
		}
		log.Info("状态已持久化", "reason", reason, "version", snap.Version)
		return nil
	}

	// 3) 调度器主循环。
	sem := make(chan struct{}, e.cfg.MaxConcurrency)
	kick := make(chan struct{}, 1)
	// kick 只是“有状态变化”的唤醒信号；真正的判据是版本号，
	// 这样过期/堆积的信号不会导致忙等，worker 在主循环阻塞期间完成也不会漏唤醒。
	var signalVer atomic.Uint64
	kicker := func() {
		signalVer.Add(1)
		select {
		case kick <- struct{}{}:
		default:
		}
	}

	// 槽位在主循环中以非阻塞方式获取：拿不到槽位的作业保持 pending，
	// 留在调度队列里等 kick，不会为每个排队作业堆积一个 goroutine。
	// len(sem) 即当前已占用槽位数，无需额外计数器。
	// activeJobs 是在运行的作业数。主循环需要“所有 worker 退出”的通知，
	// 但不能用 WaitGroup.Wait 与 Add 并发（其文档明确禁止在 Wait 期间 Add）。
	// 这里用原子计数 + allIdle 通道自管：计数归零时 worker 负责发信号。
	var activeJobs atomic.Int32
	allIdle := make(chan struct{}, 1)
	// inFlight 是当前已占用并发槽位数的原子计数，供主循环在不触碰 sem
	// 内部状态的情况下读取，避免与 worker 的槽位收发产生数据竞争。
	var inFlight atomic.Int32
	// startJob 启动一个已经持有空闲槽位的作业。
	startJob := func(j Job, st *JobState) {
		activeJobs.Add(1)
		go func() {
			defer func() {
				<-sem // 归还槽位
				inFlight.Add(-1)
				if activeJobs.Add(-1) == 0 {
					select {
					case allIdle <- struct{}{}:
					default:
					}
				}
				kicker()
			}()
			e.execute(ctx, j, st, &mu, persist, log)
		}()
	}

	log.Info("开始调度",
		"total", len(defs),
		"resumed_succeeded", resumed.succeeded,
		"reset_running", resumed.running,
		"retry_failed", resumed.failed,
		"max_concurrency", e.cfg.MaxConcurrency)

	for {
		mu.Lock()
		ready, blocked, terminal := e.plan(defs, state)
		mu.Unlock()

		if terminal {
			mu.Lock()
			nSucceeded := countStatus(state, StatusSucceeded)
			nFailed := countStatus(state, StatusFailed)
			nCanceled := countStatus(state, StatusCanceled)
			nSkipped := countStatus(state, StatusSkipped)
			mu.Unlock()
			log.Info("全部作业已进入终态，调度结束",
				"succeeded", nSucceeded,
				"failed", nFailed,
				"canceled", nCanceled,
				"skipped", nSkipped)
			break
		}

		dispatched := 0
		slotFull := false
		for _, id := range ready {
			// 非阻塞抢槽：并发已满时，剩余就绪作业保持 pending 排队，
			// 等运行中的作业归还槽位后由 kick 重新评估。
			select {
			case sem <- struct{}{}:
				inFlight.Add(1)
			default:
				slotFull = true
			}
			if slotFull {
				// 后面的就绪作业本轮一律不再派发，直接进入等待，避免空转。
				break
			}

			mu.Lock()
			// 双重确认：拿锁后状态可能已被 worker 改写。
			st := state.Jobs[id]
			if st == nil || st.Status != StatusPending {
				mu.Unlock()
				<-sem // 释放刚刚抢到的槽位
				inFlight.Add(-1)
				continue
			}
			now := time.Now().UTC()
			st.Status = StatusRunning
			st.FailureCode = FailureNone
			st.StartedAt = &now
			st.UpdatedAt = &now
			j := defs[id]
			mu.Unlock()

			if err := persist("job_dispatched:" + string(id)); err != nil {
				// 落盘失败意味着无法保证幂等恢复，直接失败该作业而非冒险执行。
				<-sem
				inFlight.Add(-1)
				mu.Lock()
				st.Status = StatusFailed
				st.FailureCode = FailureJobError
				st.FailureMsg = "调度前状态持久化失败: " + err.Error()
				t := time.Now().UTC()
				st.FinishedAt = &t
				mu.Unlock()
				_ = persist("job_dispatch_persist_failed:" + string(id))
				kicker()
				continue
			}
			log.Info("派发作业", "job", id, "deps", j.Deps,
				"attempt", st.Attempts+1, "slots_in_use", inFlight.Load())
			dispatched++
			startJob(j, st)
		}
		if slotFull {
			log.Info("并发上限已达，其余就绪作业排队等待",
				"queued_ready", len(ready)-dispatched, "slots_in_use", inFlight.Load())
		}

		// blocked 中那些上游已确定不会成功的作业，直接判 skipped。
		if len(blocked) > 0 {
			if e.markSkipped(blocked, defs, state, &mu, persist, log) {
				continue
			}
		}

		// 没有在跑的作业、也没有可推进的 pending -> 结构性死局（正常不应发生，DAG 已校验）。
		mu.Lock()
		pendingLeft := hasPending(state)
		mu.Unlock()
		if inFlight.Load() == 0 && !pendingLeft {
			mu.Lock()
			for id, st := range state.Jobs {
				if st.Status != StatusSucceeded {
					log.Error("作业在无运行中任务时仍非终态，标记为失败",
						"job", id, "status", st.Status)
					st.Status = StatusFailed
					st.FailureCode = FailureJobError
					st.FailureMsg = "scheduler: 无可运行任务但作业未到达终态"
					t := time.Now().UTC()
					st.FinishedAt = &t
				}
			}
			mu.Unlock()
			_ = persist("scheduler_deadlock_guard")
			continue
		}

		// 阻塞等待协议（既不漏唤醒也不忙等）：
		//  1. 记录当前信号版本 verBefore；
		//  2. 非阻塞排空通道——这些信号对应的状态变化本轮 plan 已经看到，
		//     属于陈旧信号；排空与阻塞之间若有 worker 完成，其信号会留在
		//     容量为 1 的通道里，阻塞 select 立刻命中，不存在丢信号窗口；
		//  3. 阻塞命中 kick 后再用版本号过滤：版本未变就是陈旧/重复信号，继续等。
		verBefore := signalVer.Load()
	drainStale:
		for {
			select {
			case <-kick:
			default:
				break drainStale
			}
		}
	waitSignal:
		for {
			select {
			case <-ctx.Done():
				break waitSignal
			case <-allIdle:
				// 所有 worker 已退出（正常情况下下一步 plan 即 terminal）。
				break waitSignal
			case <-kick:
				if signalVer.Load() != verBefore {
					break waitSignal
				}
				// 陈旧/重复信号，继续等待。
			}
		}

		if ctx.Err() != nil {
			// 批次取消/超时：把取消信号传播给正在运行的作业（通过 ctx），
			// 未启动的 pending 作业落为 canceled 终态，不留中间态。
			log.Warn("批次收到取消/超时信号，传播取消", "err", ctx.Err())
			mu.Lock()
			anyRunning := false
			for _, st := range state.Jobs {
				if st.Status == StatusRunning {
					anyRunning = true
				}
				if st.Status == StatusPending {
					now := time.Now().UTC()
					st.Status = StatusCanceled
					st.FailureCode = FailureCanceled
					st.FailureMsg = "批次在作业启动前被取消: " + ctx.Err().Error()
					st.FinishedAt = &now
					st.UpdatedAt = &now
				}
			}
			mu.Unlock()
			_ = persist("batch_canceled_pending")
			if !anyRunning {
				return e.buildResult(state, &mu, log), nil
			}
			// 等在跑的作业感知 ctx 取消并收尾；它们会 persist 终态并 kick。
			select {
			case <-allIdle:
			case <-time.After(waitRunningShutdown):
				log.Error("部分作业未在宽限期内响应取消", "grace", waitRunningShutdown)
			}
			return e.buildResult(state, &mu, log), nil
		}
	}

	return e.buildResult(state, &mu, log), nil
}

// waitRunningShutdown 是取消后等待运行中作业自行收尾的宽限期。
const waitRunningShutdown = 5 * time.Second

// recoverySummary 记录恢复路径上各类作业的数量，仅用于日志。
type recoverySummary struct {
	succeeded, running, failed, newJobs int
}

// recoverState 把历史状态与本次定义对齐：
//   - succeeded：保持终态，绝不重新执行（幂等恢复的核心保证）；
//   - running：上次进程中断留下的残留，重置为 pending 重新调度；
//   - failed/canceled/skipped：非成功终态，重新提交时给一次新的尝试机会，重置为 pending；
//   - 定义中新增的作业：初始化为 pending。
func (e *Engine) recoverState(state *BatchState, defs map[JobID]Job, log *slog.Logger) recoverySummary {
	var sum recoverySummary
	for id := range defs {
		st, ok := state.Jobs[id]
		if !ok {
			state.Jobs[id] = &JobState{ID: id, Status: StatusPending}
			sum.newJobs++
			continue
		}
		st.ID = id
		switch st.Status {
		case StatusSucceeded:
			sum.succeeded++
			log.Info("恢复：作业此前已成功，跳过执行（幂等）", "job", id, "attempts", st.Attempts)
		case StatusRunning:
			sum.running++
			log.Warn("恢复：发现中断残留的 running 状态，重置为 pending",
				"job", id, "attempts", st.Attempts)
			st.Status = StatusPending
			st.FailureCode = FailureNone
			st.FailureMsg = ""
			st.StartedAt = nil
			st.FinishedAt = nil
		case StatusFailed, StatusCanceled, StatusSkipped:
			sum.failed++
			log.Info("恢复：作业此前为非成功终态，重新提交后重新排队",
				"job", id, "previous", st.Status, "previous_failure", st.FailureCode)
			st.Status = StatusPending
			st.Attempts = 0
			st.FailureCode = FailureNone
			st.LastFailure = FailureNone
			st.FailureMsg = ""
			st.StartedAt = nil
			st.FinishedAt = nil
		default:
			// 未知状态值：保守地重置为 pending。
			log.Warn("恢复：遇到未知状态，重置为 pending", "job", id, "status", st.Status)
			st.Status = StatusPending
		}
	}
	// 历史状态里存在、但本次定义已删除的作业：保留快照不动，不参与调度。
	return sum
}

// plan 在当前快照上判定：
//   - ready：所有依赖均已 succeeded、自身 pending 的作业（已按 ID 排序，保证判定可观测、确定）；
//   - blocked：自身 pending 但至少一个依赖尚未成功的作业；
//   - terminal：不存在任何 pending/running 作业。
func (e *Engine) plan(defs map[JobID]Job, state *BatchState) (ready, blocked []JobID, terminal bool) {
	active := 0
	for id := range defs {
		st := state.Jobs[id]
		if st == nil {
			continue
		}
		switch st.Status {
		case StatusPending:
			depsOK := true
			for _, dep := range defs[id].Deps {
				dst := state.Jobs[dep]
				if dst == nil || dst.Status != StatusSucceeded {
					depsOK = false
					break
				}
			}
			if depsOK {
				ready = append(ready, id)
			} else {
				blocked = append(blocked, id)
			}
		case StatusRunning:
			active++
		}
	}
	slices.Sort(ready)
	slices.Sort(blocked)
	return ready, blocked, active == 0 && len(ready) == 0 && len(blocked) == 0
}

// markSkipped 把 blocked 中“已确定永远等不到上游成功”的作业落为 skipped。
// 返回是否发生了状态变更。
func (e *Engine) markSkipped(blocked []JobID, defs map[JobID]Job, state *BatchState,
	mu *sync.Mutex, persist func(string) error, log *slog.Logger) bool {

	upstreamDead := func(id JobID) (JobID, bool) {
		for _, dep := range defs[id].Deps {
			dst := state.Jobs[dep]
			if dst == nil {
				continue
			}
			switch dst.Status {
			case StatusFailed, StatusCanceled, StatusSkipped:
				return dep, true
			}
		}
		return "", false
	}

	changed := false
	var toSkip []struct{ id, dep JobID }
	mu.Lock()
	for _, id := range blocked {
		st := state.Jobs[id]
		if st == nil || st.Status != StatusPending {
			continue
		}
		if dep, dead := upstreamDead(id); dead {
			toSkip = append(toSkip, struct{ id, dep JobID }{id, dep})
		}
	}
	now := time.Now().UTC()
	for _, x := range toSkip {
		st := state.Jobs[x.id]
		st.Status = StatusSkipped
		st.FailureCode = FailureUpstreamFailed
		st.FailureMsg = fmt.Sprintf("上游作业 %q 状态为 %s，本作业不再执行",
			x.dep, state.Jobs[x.dep].Status)
		st.FinishedAt = &now
		st.UpdatedAt = &now
		changed = true
		log.Info("作业因上游终态被跳过",
			"job", x.id, "upstream", x.dep,
			"upstream_status", state.Jobs[x.dep].Status)
	}
	mu.Unlock()
	if changed {
		_ = persist("jobs_skipped_after_upstream_terminal")
	}
	return changed
}

// execute 负责单个作业的全部尝试：超时、重试、退避、终态落盘。
// 状态的每一次跃迁都在 mu 保护下进行并立即持久化。
func (e *Engine) execute(batchCtx context.Context, j Job, st *JobState,
	mu *sync.Mutex, persist func(string) error, log *slog.Logger) {

	policy := normalizeRetry(e.cfg.DefaultRetry)
	jobCtx, cancel := context.WithCancel(batchCtx)
	defer cancel()

	for {
		// 批次已取消则不再发起新的尝试。
		if batchCtx.Err() != nil {
			e.finish(st, mu, StatusCanceled, FailureCanceled,
				"批次已取消: "+batchCtx.Err().Error(), persist, log)
			return
		}

		attempt := 0
		mu.Lock()
		st.Attempts++
		attempt = st.Attempts
		now := time.Now().UTC()
		st.UpdatedAt = &now
		mu.Unlock()
		_ = persist("job_attempt_started:" + string(j.ID))

		log.Info("作业开始尝试", "job", j.ID, "attempt", attempt,
			"max_attempts", policy.MaxAttempts, "timeout", j.AttemptTimeout)

		code, msg := e.runAttempt(jobCtx, j)

		mu.Lock()
		st.LastFailure = code
		now2 := time.Now().UTC()
		st.UpdatedAt = &now2
		mu.Unlock()

		if code == FailureNone {
			log.Info("作业尝试成功", "job", j.ID, "attempt", attempt)
			e.finish(st, mu, StatusSucceeded, FailureNone, "", persist, log)
			return
		}

		// 批次取消优先于任何单次失败/超时结论。
		if batchCtx.Err() != nil {
			log.Warn("作业尝试结束但批次已取消，归为 canceled",
				"job", j.ID, "attempt", attempt, "direct_outcome", code)
			e.finish(st, mu, StatusCanceled, FailureCanceled,
				"批次取消（本次尝试直接结果: "+string(code)+"）: "+msg, persist, log)
			return
		}

		log.Warn("作业尝试失败",
			"job", j.ID, "attempt", attempt,
			"failure_code", code, "msg", msg)

		if attempt >= policy.MaxAttempts {
			e.finish(st, mu, StatusFailed, FailureRetriesExhausted,
				fmt.Sprintf("重试次数用尽（%d/%d），最后一次原因=%s: %s",
					attempt, policy.MaxAttempts, code, msg),
				persist, log)
			return
		}

		wait := backoff(policy, attempt)
		log.Info("计划重试", "job", j.ID,
			"next_attempt", attempt+1, "backoff", wait)

		// 退避期间也要响应取消，避免取消后还在睡眠。
		timer := time.NewTimer(wait)
		select {
		case <-batchCtx.Done():
			timer.Stop()
			e.finish(st, mu, StatusCanceled, FailureCanceled,
				"退避期间批次被取消: "+batchCtx.Err().Error(), persist, log)
			return
		case <-timer.C:
		}
	}
}

// runAttempt 执行作业函数一次，返回可区分的直接结果：
//   - FailureNone：成功；
//   - FailureTimeout：达到作业自身的 AttemptTimeout；
//   - FailureCanceled：ctx 被取消；
//   - FailureJobError：函数返回错误（含 panic 恢复）。
func (e *Engine) runAttempt(ctx context.Context, j Job) (FailureCode, string) {
	resultCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				resultCh <- fmt.Errorf("作业 panic: %v", r)
			}
		}()
		resultCh <- j.Fn(ctx)
	}()

	var timeoutCh <-chan time.Time
	if j.AttemptTimeout > 0 {
		timer := time.NewTimer(j.AttemptTimeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	// 显式优先级：当结果与取消/超时同时就绪时，优先采用作业函数的明确结果，
	// 避免“作业已完成副作用却被记为取消/超时”后重启重复执行。
	// Go 的 select 在多 case 就绪时随机选择，因此先做一次非阻塞结果判定。
	select {
	case err := <-resultCh:
		if err != nil {
			return FailureJobError, err.Error()
		}
		return FailureNone, ""
	default:
	}

	select {
	case err := <-resultCh:
		if err != nil {
			return FailureJobError, err.Error()
		}
		return FailureNone, ""
	case <-ctx.Done():
		return FailureCanceled, "作业上下文被取消: " + ctx.Err().Error()
	case <-timeoutCh:
		return FailureTimeout, "作业超过单次执行超时 " + j.AttemptTimeout.String()
	}
}

// finish 在锁保护下写入终态并立即持久化，保证每个终态都是明确、可恢复的记录。
func (e *Engine) finish(st *JobState, mu *sync.Mutex, status Status,
	code FailureCode, msg string, persist func(string) error, log *slog.Logger) {
	mu.Lock()
	now := time.Now().UTC()
	st.Status = status
	st.FailureCode = code
	st.FailureMsg = msg
	st.UpdatedAt = &now
	st.FinishedAt = &now
	if status == StatusSucceeded {
		st.LastFailure = FailureNone
	}
	mu.Unlock()
	reason := "job_terminal:" + string(st.ID) + ":" + string(status)
	if err := persist(reason); err != nil {
		// 终态落盘失败：重试一次同步写，仍失败则让状态保留在内存并显式报错，
		// 绝不静默继续，避免出现“内存已终态、磁盘未记录”的不明确状态。
		log.Error("终态持久化失败，进行一次重试", "job", st.ID, "status", status, "err", err)
		if err2 := persist(reason + ":retry"); err2 != nil {
			log.Error("终态持久化重试仍失败", "job", st.ID, "err", err2)
		}
	}
	log.Info("作业到达终态",
		"job", st.ID, "status", status,
		"failure_code", code, "attempts", st.Attempts)
}

// cloneStateLocked 在持有状态锁时生成一份深拷贝，供锁外序列化使用。
// 时间字段虽为指针，但创建后从不原地修改（每次都赋新的 time.Time），
// 因此逐 JobState 做结构体值拷贝即可安全隔离。
func cloneStateLocked(s *BatchState) *BatchState {
	cp := &BatchState{
		BatchID:   s.BatchID,
		Version:   s.Version,
		UpdatedAt: s.UpdatedAt,
		Jobs:      make(map[JobID]*JobState, len(s.Jobs)),
	}
	for id, js := range s.Jobs {
		jobCopy := *js
		cp.Jobs[id] = &jobCopy
	}
	return cp
}

func (e *Engine) buildResult(state *BatchState, mu *sync.Mutex, log *slog.Logger) *BatchResult {
	// 在锁内基于快照构建结果：取消宽限期耗尽时仍可能有 worker 在收尾。
	mu.Lock()
	snap := cloneStateLocked(state)
	mu.Unlock()

	res := &BatchResult{BatchID: snap.BatchID, State: snap, AllSucceeded: true}
	for _, st := range snap.Jobs {
		if st.Status != StatusSucceeded {
			res.AllSucceeded = false
			res.Failed = append(res.Failed, *st)
		}
	}
	slices.SortFunc(res.Failed, func(a, b JobState) int { return strings.Compare(string(a.ID), string(b.ID)) })
	log.Info("批次结束",
		"all_succeeded", res.AllSucceeded,
		"non_succeeded", len(res.Failed), "version", snap.Version)
	return res
}

// State 返回某批次最近一次持久化的状态（不存在时返回 ErrStateNotFound）。
func (e *Engine) State(ctx context.Context, batchID BatchID) (*BatchState, error) {
	st, err := e.store.Load(ctx, batchID)
	if err != nil {
		return nil, err
	}
	// 返回副本，避免调用方修改引擎内部状态。
	cp := &BatchState{BatchID: st.BatchID, Version: st.Version, UpdatedAt: st.UpdatedAt,
		Jobs: make(map[JobID]*JobState, len(st.Jobs))}
	for id, js := range st.Jobs {
		jobCopy := *js
		cp.Jobs[id] = &jobCopy
	}
	return cp, nil
}

func joinValidationErrs(errs []ValidationError) string {
	msgs := make([]string, 0, len(errs))
	for _, ve := range errs {
		msgs = append(msgs, ve.Message)
	}
	return strings.Join(msgs, "; ")
}

func countStatus(state *BatchState, s Status) int {
	n := 0
	for _, st := range state.Jobs {
		if st.Status == s {
			n++
		}
	}
	return n
}

func hasPending(state *BatchState) bool {
	for _, st := range state.Jobs {
		if st.Status == StatusPending || st.Status == StatusRunning {
			return true
		}
	}
	return false
}
