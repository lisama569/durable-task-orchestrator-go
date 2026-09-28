package orchestrator

import (
	"context"
	"time"
)

// JobID 是单个作业在同一批次内的唯一标识。
type JobID string

// BatchID 是一次任务批提交的幂等标识：相同 BatchID 的重复提交
// 会复用同一份持久化状态，已成功的作业不会再次执行。
type BatchID string

// Status 表示作业在其生命周期中所处的明确状态。
type Status string

const (
	// StatusPending 已校验、等待依赖完成或等待并发槽位。
	StatusPending Status = "pending"
	// StatusRunning 作业函数正在执行（进程重启时该状态会被判定为残留并重置）。
	StatusRunning Status = "running"
	// StatusSucceeded 作业成功完成，结果已持久化，永不再执行。
	StatusSucceeded Status = "succeeded"
	// StatusFailed 作业在重试策略用尽后最终失败，或因不可重试原因失败。
	StatusFailed Status = "failed"
	// StatusCanceled 批次被取消或整体超时，作业未取得成功结果。
	StatusCanceled Status = "canceled"
	// StatusSkipped 作业的某个上游最终失败/被取消，因而不再被调度。
	StatusSkipped Status = "skipped"
)

// FailureCode 用于区分不同的失败原因，便于恢复时做出不同决策。
type FailureCode string

const (
	FailureNone             FailureCode = ""
	FailureJobError         FailureCode = "job_error"         // 作业函数返回错误
	FailureTimeout          FailureCode = "timeout"           // 作业单次执行超时
	FailureRetriesExhausted FailureCode = "retries_exhausted" // 重试次数用尽
	FailureCanceled         FailureCode = "canceled"          // 批次/作业被取消
	FailureUpstreamFailed   FailureCode = "upstream_failed"   // 上游最终失败导致跳过
)

// RetryPolicy 描述失败/超时后的重试策略。
type RetryPolicy struct {
	// MaxAttempts 最大执行次数（含首次）。<=1 表示不重试。
	MaxAttempts int
	// InitialBackoff 首次重试前的基础退避时间。
	InitialBackoff time.Duration
	// MaxBackoff 退避上限。
	MaxBackoff time.Duration
	// Multiplier 退避乘数，指数退避：next = min(prev*Multiplier, MaxBackoff)。
	Multiplier float64
}

// DefaultRetryPolicy 返回“不重试”的保守默认策略。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1, Multiplier: 2}
}

// Job 是一个待调度的作业定义。
type Job struct {
	ID             JobID
	Deps           []JobID       // 必须先成功完成的上游作业
	AttemptTimeout time.Duration // 单次执行超时；0 表示不设单次超时
	Fn             func(ctx context.Context) error
}

// JobState 是单个作业的持久化状态快照。
type JobState struct {
	ID          JobID       `json:"id"`
	Status      Status      `json:"status"`
	Attempts    int         `json:"attempts"`     // 已开始执行的次数
	FailureCode FailureCode `json:"failure_code"` // 非成功终态时的可区分原因
	LastFailure FailureCode `json:"last_failure"` // retries_exhausted 时最后一次尝试的直接原因（job_error/timeout）
	FailureMsg  string      `json:"failure_msg"`
	StartedAt   *time.Time  `json:"started_at,omitempty"`
	UpdatedAt   *time.Time  `json:"updated_at,omitempty"`
	FinishedAt  *time.Time  `json:"finished_at,omitempty"`
}

// BatchState 是整批任务的持久化状态。
type BatchState struct {
	BatchID   BatchID             `json:"batch_id"`
	Version   int                 `json:"version"` // 每次持久化递增，用于乐观并发
	Jobs      map[JobID]*JobState `json:"jobs"`
	UpdatedAt time.Time           `json:"updated_at"`
}

// BatchResult 是一次 Run 结束后对整批结果的汇总。
type BatchResult struct {
	BatchID BatchID
	State   *BatchState
	// AllSucceeded 为 true 当且仅当所有作业均为 succeeded。
	AllSucceeded bool
	// Failed 列出最终失败/取消/跳过的作业状态。
	Failed []JobState
}

// ValidationError 在启动阶段描述 DAG 定义中的错误位置。
type ValidationError struct {
	Kind    string // "unknown_dependency" | "cycle" | "duplicate_job" | "empty_id"
	JobID   JobID  // 触发问题的作业
	DepID   JobID  // 若是依赖引用问题，指向被引用的作业
	Message string
}

func (e *ValidationError) Error() string { return e.Message }
