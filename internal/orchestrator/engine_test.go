package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestEngine(t *testing.T, cfg Config) (*Engine, *FileStore) {
	t.Helper()
	dir := t.TempDir()
	store := NewFileStore(dir)
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(&testWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return NewEngine(store, cfg), store
}

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

func job(id JobID, deps []JobID, fn func(ctx context.Context) error) Job {
	return Job{ID: id, Deps: deps, Fn: fn}
}

func okJob(id JobID, deps ...JobID) Job {
	return job(id, deps, func(ctx context.Context) error { return nil })
}

func statusOf(st *BatchState, id JobID) Status {
	return st.Jobs[id].Status
}

func TestRun_BasicDiamondDAG(t *testing.T) {
	e, _ := newTestEngine(t, Config{MaxConcurrency: 4})
	var orderMu sync.Mutex
	var order []string
	rec := func(id string) {
		orderMu.Lock()
		order = append(order, id)
		orderMu.Unlock()
	}
	jobs := []Job{
		job("root", nil, func(ctx context.Context) error { rec("root"); return nil }),
		job("left", []JobID{"root"}, func(ctx context.Context) error { rec("left"); return nil }),
		job("right", []JobID{"root"}, func(ctx context.Context) error { rec("right"); return nil }),
		job("top", []JobID{"left", "right"}, func(ctx context.Context) error { rec("top"); return nil }),
	}
	res, err := e.Run(context.Background(), "b1", jobs)
	if err != nil {
		t.Fatalf("Run 返回错误: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("期望全部成功: %+v", res.Failed)
	}
	if len(order) != 4 {
		t.Fatalf("期望执行 4 个作业，实际 %d", len(order))
	}
	idx := map[string]int{}
	for i, s := range order {
		idx[s] = i
	}
	if !(idx["root"] < idx["left"] && idx["root"] < idx["right"]) {
		t.Fatalf("root 必须先于 left/right: %v", order)
	}
	if !(idx["left"] < idx["top"] && idx["right"] < idx["top"]) {
		t.Fatalf("left/right 必须先于 top: %v", order)
	}
}

func TestRun_RejectsCycleBeforeStarting(t *testing.T) {
	e, _ := newTestEngine(t, Config{})
	var executed atomic.Int32
	jobs := []Job{
		job("a", []JobID{"b"}, func(ctx context.Context) error { executed.Add(1); return nil }),
		job("b", []JobID{"a"}, func(ctx context.Context) error { executed.Add(1); return nil }),
	}
	_, err := e.Run(context.Background(), "b-cycle", jobs)
	if err == nil {
		t.Fatal("期望循环依赖被拒绝")
	}
	var msg string
	for _, ve := range ValidateDAG(jobs) {
		msg += ve.Message
	}
	if executed.Load() != 0 {
		t.Fatalf("校验失败时不应执行任何作业，实际执行 %d 次", executed.Load())
	}
	if !contains(err.Error(), "循环依赖") {
		t.Fatalf("错误信息应说明循环依赖，得到: %v", err)
	}
	_ = msg
}

func TestRun_RejectsUnknownDependency(t *testing.T) {
	e, _ := newTestEngine(t, Config{})
	_, err := e.Run(context.Background(), "b-unknown", []Job{
		job("a", []JobID{"nope"}, func(ctx context.Context) error { return nil }),
	})
	if err == nil || !contains(err.Error(), "不存在的依赖") {
		t.Fatalf("期望明确指出未知依赖，得到: %v", err)
	}
}

func TestRun_RetriesExhausted_DistinctFailureCode(t *testing.T) {
	e, _ := newTestEngine(t, Config{
		MaxConcurrency: 1,
		DefaultRetry: RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
			Multiplier:     2,
		},
	})
	var attempts atomic.Int32
	jobs := []Job{
		job("flaky", nil, func(ctx context.Context) error {
			attempts.Add(1)
			return errors.New("boom")
		}),
	}
	res, err := e.Run(context.Background(), "b-retry", jobs)
	if err != nil {
		t.Fatalf("Run 不应因作业失败返回 Go error: %v", err)
	}
	if res.AllSucceeded {
		t.Fatal("期望批次非全成功")
	}
	st := res.State.Jobs["flaky"]
	if st.Status != StatusFailed {
		t.Fatalf("期望 failed，得到 %s", st.Status)
	}
	if st.FailureCode != FailureRetriesExhausted {
		t.Fatalf("期望失败原因为 retries_exhausted，得到 %s", st.FailureCode)
	}
	if st.LastFailure != FailureJobError {
		t.Fatalf("期望最后一次直接原因为 job_error，得到 %s", st.LastFailure)
	}
	if attempts.Load() != 3 {
		t.Fatalf("期望恰好尝试 3 次，实际 %d", attempts.Load())
	}
	if !contains(st.FailureMsg, "重试次数用尽") {
		t.Fatalf("失败信息应说明重试耗尽: %s", st.FailureMsg)
	}
}

func TestRun_RetryThenSucceed(t *testing.T) {
	e, _ := newTestEngine(t, Config{
		DefaultRetry: RetryPolicy{MaxAttempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond},
	})
	var n atomic.Int32
	res, err := e.Run(context.Background(), "b-retry-ok", []Job{
		job("j", nil, func(ctx context.Context) error {
			if n.Add(1) < 3 {
				return errors.New("not yet")
			}
			return nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.AllSucceeded || n.Load() != 3 {
		t.Fatalf("期望第 3 次尝试成功，attempts=%d allOK=%v", n.Load(), res.AllSucceeded)
	}
}

func TestRun_TimeoutMarkedAndRetried(t *testing.T) {
	e, _ := newTestEngine(t, Config{
		DefaultRetry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
	})
	var n atomic.Int32
	j := Job{
		ID: "slow",
		Fn: func(ctx context.Context) error {
			if n.Add(1) == 1 {
				select {} // 永不返回，依赖超时/取消来终结
			}
			return nil
		},
		AttemptTimeout: 30 * time.Millisecond,
	}
	res, err := e.Run(context.Background(), "b-timeout", []Job{j})
	if err != nil {
		t.Fatal(err)
	}
	st := res.State.Jobs["slow"]
	if st.Status != StatusSucceeded {
		t.Fatalf("期望超时重试后成功，得到 %s code=%s msg=%s", st.Status, st.FailureCode, st.FailureMsg)
	}

	// 全部超时 -> 失败原因可区分：retries_exhausted + last_failure=timeout
	var m atomic.Int32
	j2 := Job{ID: "slow2", AttemptTimeout: 20 * time.Millisecond, Fn: func(ctx context.Context) error {
		m.Add(1)
		<-ctx.Done() // 等待超时取消
		return ctx.Err()
	}}
	res2, err := e.Run(context.Background(), "b-timeout2", []Job{j2})
	if err != nil {
		t.Fatal(err)
	}
	st2 := res2.State.Jobs["slow2"]
	if st2.Status != StatusFailed || st2.FailureCode != FailureRetriesExhausted || st2.LastFailure != FailureTimeout {
		t.Fatalf("期望重试用尽且最后直接原因为 timeout，得到 status=%s code=%s last=%s",
			st2.Status, st2.FailureCode, st2.LastFailure)
	}
	if st2.Attempts != 2 {
		t.Fatalf("期望尝试 2 次，得到 %d", st2.Attempts)
	}
}

func TestRun_UpstreamFailureSkipsDownstream(t *testing.T) {
	e, _ := newTestEngine(t, Config{DefaultRetry: RetryPolicy{MaxAttempts: 1}})
	downRan := make(chan struct{}, 1)
	jobs := []Job{
		job("bad", nil, func(ctx context.Context) error { return errors.New("nope") }),
		job("child", []JobID{"bad"}, func(ctx context.Context) error {
			downRan <- struct{}{}
			return nil
		}),
		job("independent", nil, func(ctx context.Context) error { return nil }),
	}
	res, err := e.Run(context.Background(), "b-skip", jobs)
	if err != nil {
		t.Fatal(err)
	}
	if res.State.Jobs["bad"].Status != StatusFailed {
		t.Fatal("bad 应失败")
	}
	if res.State.Jobs["child"].Status != StatusSkipped ||
		res.State.Jobs["child"].FailureCode != FailureUpstreamFailed {
		st := res.State.Jobs["child"]
		t.Fatalf("child 应被标记 skipped/upstream_failed，得到 %s/%s", st.Status, st.FailureCode)
	}
	if res.State.Jobs["independent"].Status != StatusSucceeded {
		t.Fatal("无关节点仍应成功")
	}
	select {
	case <-downRan:
		t.Fatal("被跳过的作业不应执行")
	default:
	}
}

func TestRun_ConcurrencyCapAndQueueing(t *testing.T) {
	const cap = 2
	e, _ := newTestEngine(t, Config{MaxConcurrency: cap})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	jobs := make([]Job, 0, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		id := JobID(fmt.Sprintf("j%d", i))
		jobs = append(jobs, job(id, nil, func(ctx context.Context) error {
			defer wg.Done()
			cur := inFlight.Add(1)
			for {
				old := maxInFlight.Load()
				if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
					break
				}
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			inFlight.Add(-1)
			return nil
		}))
	}
	done := make(chan struct{})
	go func() {
		_, _ = e.Run(context.Background(), "b-cap", jobs)
		close(done)
	}()
	// 等待作业进入运行态后检查峰值。
	time.Sleep(80 * time.Millisecond)
	if got := maxInFlight.Load(); got > cap {
		t.Fatalf("并发峰值 %d 超过上限 %d", got, cap)
	}
	if got := inFlight.Load(); got != cap {
		t.Fatalf("期望满载 %d 个作业，实际在跑 %d", cap, got)
	}
	close(release)
	wg.Wait()
	<-done
}

func TestRun_CancellationPropagates(t *testing.T) {
	e, _ := newTestEngine(t, Config{MaxConcurrency: 1})
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan Status, 1)
	jobs := []Job{
		job("long", nil, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}),
		job("queued", nil, func(ctx context.Context) error { return nil }),
	}
	go func() {
		res, _ := e.Run(ctx, "b-cancel", jobs)
		finished <- res.State.Jobs["long"].Status
	}()
	<-started
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case s := <-finished:
		if s != StatusCanceled {
			t.Fatalf("期望 long 被取消，得到 %s", s)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("取消后运行中作业未在宽限期内结束")
	}
	st, _ := e.State(context.Background(), "b-cancel")
	if st.Jobs["queued"].Status != StatusCanceled {
		t.Fatalf("期望排队作业落为 canceled，得到 %s", st.Jobs["queued"].Status)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(sub) > 0 && stringContains(s, sub)))
}

func stringContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestRun_EmptyBatch(t *testing.T) {
	e, _ := newTestEngine(t, Config{})
	res, err := e.Run(context.Background(), "empty", nil)
	if err != nil {
		t.Fatalf("空批次不应报错: %v", err)
	}
	if !res.AllSucceeded || len(res.Failed) != 0 {
		t.Fatalf("空批次应全部成功且无失败项: %+v", res)
	}
}

func TestRun_MissingBatchIDRejected(t *testing.T) {
	e, _ := newTestEngine(t, Config{})
	if _, err := e.Run(context.Background(), "", []Job{okJob("a")}); err == nil {
		t.Fatal("空 BatchID 必须被拒绝")
	}
}

func TestRun_NilFnRejected(t *testing.T) {
	e, _ := newTestEngine(t, Config{})
	_, err := e.Run(context.Background(), "nilfn", []Job{{ID: "a"}})
	if err == nil || !contains(err.Error(), "执行函数") {
		t.Fatalf("缺少 Fn 的作业应被拒绝，得到: %v", err)
	}
}
