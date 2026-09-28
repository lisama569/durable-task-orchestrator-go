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

func newTestLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(&testWriter{t: t},
		&slog.HandlerOptions{Level: slog.LevelDebug}))
}

// 重复提交同一 BatchID：已成功的作业绝不再次执行，未完成的作业继续推进。
func TestRun_DuplicateSubmission_Idempotent(t *testing.T) {
	e, _ := newTestEngine(t, Config{
		MaxConcurrency: 2,
		DefaultRetry:   RetryPolicy{MaxAttempts: 1},
	})

	var aRuns, bRuns, cRuns atomic.Int32
	buildJobs := func() []Job {
		return []Job{
			job("a", nil, func(ctx context.Context) error { aRuns.Add(1); return nil }),
			// b 在第一轮失败，第二轮（重新提交）再成功。
			job("b", nil, func(ctx context.Context) error {
				if bRuns.Add(1) == 1 {
					return errors.New("first-round failure")
				}
				return nil
			}),
			job("c", []JobID{"b"}, func(ctx context.Context) error { cRuns.Add(1); return nil }),
		}
	}

	res1, err := e.Run(context.Background(), "dup-batch", buildJobs())
	if err != nil || res1.AllSucceeded {
		t.Fatalf("第一轮期望部分失败，err=%v allOK=%v", err, res1.AllSucceeded)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 1 || cRuns.Load() != 0 {
		t.Fatalf("第一轮执行次数异常: a=%d b=%d c=%d", aRuns.Load(), bRuns.Load(), cRuns.Load())
	}

	// 重新提交同一批：a 必须跳过，b 重新尝试并成功，c 随之执行一次。
	res2, err := e.Run(context.Background(), "dup-batch", buildJobs())
	if err != nil {
		t.Fatal(err)
	}
	if !res2.AllSucceeded {
		t.Fatalf("第二轮期望全部成功: %+v", res2.Failed)
	}
	if aRuns.Load() != 1 {
		t.Fatalf("成功作业 a 被重复执行，实际 %d 次", aRuns.Load())
	}
	if bRuns.Load() != 2 {
		t.Fatalf("失败作业 b 应重新尝试 1 次，实际总执行 %d 次", bRuns.Load())
	}
	if cRuns.Load() != 1 {
		t.Fatalf("下游作业 c 应只执行 1 次，实际 %d 次", cRuns.Load())
	}

	// 第三轮：全部成功作业都应跳过，零执行。
	res3, err := e.Run(context.Background(), "dup-batch", buildJobs())
	if err != nil || !res3.AllSucceeded {
		t.Fatalf("第三轮期望全部成功且无副作用，err=%v", err)
	}
	if aRuns.Load() != 1 || bRuns.Load() != 2 || cRuns.Load() != 1 {
		t.Fatalf("第三轮出现重复执行: a=%d b=%d c=%d", aRuns.Load(), bRuns.Load(), cRuns.Load())
	}
}

// 模拟进程中断：磁盘上残留 running 状态时，重启后必须重置为 pending 并跑完，
// 而已成功的作业保持幂等。
func TestRun_RecoverFromInterruptedRunning(t *testing.T) {
	e, store := newTestEngine(t, Config{MaxConcurrency: 4})

	var runs atomic.Int32
	jobs := []Job{
		job("done", nil, func(ctx context.Context) error { runs.Add(1); return nil }),
		job("interrupted", nil, func(ctx context.Context) error { runs.Add(1); return nil }),
		job("child", []JobID{"interrupted"}, func(ctx context.Context) error { runs.Add(1); return nil }),
	}

	// 第一轮正常跑完，然后手工把 interrupted 篡改为“中断时正在运行”的残留状态，
	// 同时保留 done 的 succeeded，模拟崩溃重启。
	res, err := e.Run(context.Background(), "crash-batch", jobs)
	if err != nil || !res.AllSucceeded {
		t.Fatalf("第一轮应全部成功: %v", err)
	}
	st, err := store.Load(context.Background(), "crash-batch")
	if err != nil {
		t.Fatal(err)
	}
	st.Jobs["interrupted"].Status = StatusRunning
	st.Jobs["interrupted"].FinishedAt = nil
	// child 是 interrupted 的下游：模拟父作业在 child 启动前崩溃。
	st.Jobs["child"].Status = StatusPending
	st.Jobs["child"].Attempts = 0
	st.Jobs["child"].FailureCode = FailureNone
	if err := store.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}

	// 重启后用同一个 store/BatchID 恢复。
	res2, err := e.Run(context.Background(), "crash-batch", jobs)
	if err != nil {
		t.Fatal(err)
	}
	if !res2.AllSucceeded {
		t.Fatalf("恢复后期望全部成功: %+v", res2.Failed)
	}
	// interrupted 崩溃前已留下 1 次尝试记录，恢复时重置为 pending 但保留
	// 历史 attempts 计数，重新调度后再尝试 1 次，总计 2 次、终态成功。
	if got := res2.State.Jobs["interrupted"].Attempts; got != 2 {
		t.Fatalf("interrupted 恢复后期望累计尝试 2 次，得到 %d", got)
	}
	if res2.State.Jobs["done"].Attempts != 1 {
		t.Fatalf("done 恢复后不应重跑，期望 attempts=1，得到 %d",
			res2.State.Jobs["done"].Attempts)
	}
	if res2.State.Jobs["done"].Status != StatusSucceeded ||
		res2.State.Jobs["interrupted"].Status != StatusSucceeded ||
		res2.State.Jobs["child"].Status != StatusSucceeded {
		t.Fatal("恢复后三个作业都应是 succeeded")
	}
}

// 同一批次被并发重复提交时，第二个提交必须被拒绝，不能两个调度器并发跑同一作业。
func TestRun_ConcurrentDuplicateRejected(t *testing.T) {
	e, _ := newTestEngine(t, Config{MaxConcurrency: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	jobs := []Job{
		job("slow", nil, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}),
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = e.Run(ctx, "concurrent-batch", jobs)
		}(i)
	}
	<-started                         // 第一个 Run 已真正进入作业执行
	time.Sleep(20 * time.Millisecond) // 给第二个 Run 时间走到重复提交检查
	cancel()
	wg.Wait()

	ok, rejected := 0, 0
	for _, err := range errs {
		if err == nil {
			ok++
		} else if contains(err.Error(), "正在执行中") {
			rejected++
		} else {
			t.Fatalf("出现非预期错误: %v", err)
		}
	}
	if ok != 1 || rejected != 1 {
		t.Fatalf("期望恰好 1 个执行、1 个被拒绝，实际执行=%d 拒绝=%d errs=%v", ok, rejected, errs)
	}
}

// 取消后重新提交：已成功的作业保持幂等，被取消的作业重新排队执行。
func TestRun_ResumeAfterCancellation(t *testing.T) {
	e, _ := newTestEngine(t, Config{MaxConcurrency: 1})

	var fastRuns, slowRuns atomic.Int32
	jobs := func() []Job {
		return []Job{
			job("fast", nil, func(ctx context.Context) error { fastRuns.Add(1); return nil }),
			job("slow", nil, func(ctx context.Context) error {
				slowRuns.Add(1)
				<-ctx.Done()
				return ctx.Err()
			}),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		_, _ = e.Run(ctx, "cancel-resume", modifySlowToSignal(jobs(), started))
		close(firstDone)
	}()
	<-started
	cancel()
	<-firstDone // 等第一轮完全收尾后再重新提交，避免与“正在执行中”保护竞争

	if fastRuns.Load() != 1 {
		t.Fatalf("取消前 fast 应已成功执行 1 次，实际 %d", fastRuns.Load())
	}

	// 重新提交：fast 不重放，slow 重新执行并正常结束。
	j2 := jobs()
	j2[1] = job("slow", nil, func(ctx context.Context) error { slowRuns.Add(1); return nil })
	res, err := e.Run(context.Background(), "cancel-resume", j2)
	if err != nil || !res.AllSucceeded {
		t.Fatalf("恢复后期望全部成功: err=%v failed=%+v", err, res.Failed)
	}
	if fastRuns.Load() != 1 {
		t.Fatalf("fast 不应重复执行，实际 %d", fastRuns.Load())
	}
	// 首轮的 slow 被 signal 包装（不计 slowRuns）；恢复轮的 slow 是 jobs()
	// 中定义的计数版本，恰好执行 1 次即成功。
	if slowRuns.Load() != 1 {
		t.Fatalf("恢复轮 slow 应重新执行 1 次，实际 %d", slowRuns.Load())
	}
}

func modifySlowToSignal(jobs []Job, started chan struct{}) []Job {
	for i := range jobs {
		if jobs[i].ID == "slow" {
			jobs[i].Fn = func(ctx context.Context) error {
				select {
				case started <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			}
		}
	}
	return jobs
}

// 并发竞争压力：大量独立小作业 + 低并发上限，检查状态终态一致性与无数据竞争。
func TestRun_StressManyJobs(t *testing.T) {
	e, _ := newTestEngine(t, Config{MaxConcurrency: 3, DefaultRetry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}})
	const n = 40
	var executions atomic.Int32
	jobs := make([]Job, 0, n)
	for i := 0; i < n; i++ {
		id := JobID(fmt.Sprintf("job-%02d", i))
		var deps []JobID
		if i > 0 && i%4 == 0 {
			deps = []JobID{JobID(fmt.Sprintf("job-%02d", i-1))}
		}
		fail := i%7 == 0 // 一部分作业首次失败
		jobs = append(jobs, job(id, deps, func(ctx context.Context) error {
			executions.Add(1)
			if fail && !seenFail(id) {
				return errors.New("transient")
			}
			return nil
		}))
	}

	res, err := e.Run(context.Background(), "stress", jobs)
	if err != nil {
		t.Fatal(err)
	}
	if !res.AllSucceeded {
		t.Fatalf("压力测试期望重试后全部成功: %+v", res.Failed)
	}
	for _, st := range res.State.Jobs {
		if st.Status != StatusSucceeded {
			t.Fatalf("作业 %s 非成功终态 %s", st.ID, st.Status)
		}
		if st.FinishedAt == nil {
			t.Fatalf("作业 %s 缺少 FinishedAt", st.ID)
		}
	}
}

var failSeen sync.Map

func seenFail(id JobID) bool {
	_, loaded := failSeen.LoadOrStore(id, struct{}{})
	return loaded
}
