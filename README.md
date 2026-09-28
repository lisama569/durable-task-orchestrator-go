# Durable Task Orchestrator (Go)

一个支持依赖调度、失败重试、并发上限、取消传播和中断恢复的任务编排引擎。
一批有依赖关系的作业在进程崩溃、重复提交和取消/超时场景下都能得到稳定、
可预期的结果。

## 包结构

| 包 | 职责 |
| --- | --- |
| `pkg/task` | 任务、状态、哨兵错误、失败原因常量 |
| `pkg/dag` | 启动期依赖图校验（环 / 缺失引用 / 重复 ID）与拓扑排序 |
| `pkg/retry` | 重试策略（固定退避 `FixedPolicy`、指数退避 `ExponentialPolicy`） |
| `pkg/store` | 持久化存储：`FileStore`（JSON + 原子 rename + flock）、`MemoryStore` |
| `pkg/orchestrator` | 调度执行引擎：依赖调度、并发上限、重试、取消、恢复 |
| `cmd/demo` | 可运行的本地演示程序 |

## 调度规则

1. **依赖先行**：一个任务只有在其 `Deps` 中列出的所有任务都达到
   `succeeded` 后才会被启动；依赖未完成的任务绝不提前执行。
2. **无依赖可并发**：当前所有满足依赖的任务会被并发派发。
3. **并发上限**：`Config.Concurrency` 限制同时在跑的任务数。到达上限时，
   新任务保持 `pending` 在内存队列中等待，**不会为排队任务创建 goroutine
   或其它资源**，因此不会无限堆积资源占用。
4. **启动期拒绝非法图**：`orchestrator.New` 在任何任务执行之前校验，以下
   情况直接返回错误，并在错误信息中指出具体位置：
   - 空 ID、重复 ID；
   - 依赖引用了不存在的任务（错误中给出 `任务 -> 缺失依赖` 这条边）；
   - 循环依赖，包括自依赖（错误中给出环上的节点序列）。
5. **失败传播**：任务终态失败后，其所有（传递）下游任务不会执行，直接
   标记为 `failed`，失败原因为 `dependency_failed`。与之无依赖关系的任务
   不受影响，继续执行。

## 重试语义

- 每个任务按 `retry.Policy` 重试；`FixedPolicy{Attempts: N}` 表示最多
  尝试 N 次（N=1 即不重试），每次失败后等待 `Delay`；`ExponentialPolicy`
  提供翻倍退避并以 `MaxDelay` 封顶。
- 退避期间任务保持 `pending`（带一个不持久化的 `ReadyAt`），不占并发名额。
- 重试预算用尽后任务进入终态 `failed`，`RunState.FailureReason` 记录**可
  区分的失败原因**：

  | FailureReason | 含义 |
  | --- | --- |
  | `retry_exhausted` | 普通错误，所有尝试均失败 |
  | `timeout` | 最后一次失败源于单次尝试超时（`Task.Timeout`）或父 deadline |
  | `dependency_failed` | 从未执行，因前置任务终态失败被跳过 |

  单次尝试的错误还可用 `errors.Is(err, task.ErrTimeout)` 判断。

## 幂等与中断恢复语义

- 每个批次用 `BatchID` 标识。状态在每个关键节点（进入 running、成功、
  失败、重置）立即持久化到 `Store`。
- **成功不重放**：已经 `succeeded` 的任务在重启后或同一批次被重复提交时
  直接跳过，不会再次执行任务体，因此不会重复产生副作用。**任务体本身仍
  应按幂等方式编写**（引擎保证 at-most-once 执行；无法在进程被 kill -9
  的瞬间替用户回滚已经产生的外部副作用）。
- **中断可恢复**：
  - 进入 running 前先落盘。进程在任务执行中途被杀死，磁盘上留下的是
    明确的 `running` 记录；下次提交时它会被重置为 `pending` 并重新执行。
    磁盘上永远不会出现含义不明的中间态。
  - 被中断的尝试**不消耗重试预算**（任务没有拿到终态结果）。
  - 通过 `context` 取消（超时/主动取消）正在运行的批次时，取消会传播到
    每个在跑任务的 ctx；引擎等待任务体响应后，把未完成任务重置为
    `pending`，本次 `Run` 返回 `context.Canceled`，下次提交继续。
- **重复提交串行化**：`FileStore` 用 `flock`、`MemoryStore` 用通道锁，对
  同一 `BatchID` 的并发执行做互斥；第二个执行者等待批次完成后复用已持久
  化的结果，保证同一任务体不会被两个执行者并发运行。
- `FileStore` 的状态文件采用「临时文件 + fsync + 原子 rename」写入，崩溃
  不会留下半截 JSON。

## 日志

引擎通过 `orchestrator.Logger` 输出结构化日志（`NewTextLogger` 写文本行，
`NopLogger` 静默，也可自行实现接口）。每次调度的关键输入和判定依据都会
打印，例如：

```
event=task_scheduled task_id=render attempt=2 deps=[fetch] decision=deps_succeeded_and_budget_available
event=task_attempt_failed_retry task_id=render attempt=1 err_kind=error backoff_ms=10 decision=retry_budget_remaining
event=task_failed_terminal task_id=render attempts=3 reason=retry_exhausted decision=retry_budget_exhausted
event=task_skipped_dependency_failed task_id=notify failed_dep=render decision=prerequisite_terminal_failure
event=recover_interrupted task_id=render previous_attempt=1 decision=reset_running_to_pending
```

## 本地验证

要求 Go 1.25+。

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全部单元测试（含循环依赖、重试耗尽、超时、中断恢复、重复提交、并发竞争）
go test ./... -race -count=1

# 只跑编排引擎测试
go test ./pkg/orchestrator/ -race -v -count=1

# 运行演示（包含一个前两次失败、第三次成功的任务）
go run ./cmd/demo -batch demo -dir ./.state
# 再次提交同一批次：所有任务已 succeeded，不会再执行任何任务体
go run ./cmd/demo -batch demo -dir ./.state
```

测试覆盖的关键场景：

- `TestValidateSimpleCycle` / `TestValidateMissingDependency` /
  `TestValidateDuplicateID`：循环依赖、缺失引用、重复 ID 在启动期被拒绝，
  且错误指明具体位置；
- `TestRetryExhaustedDistinguishesReason` / `TestTimeoutFailureReason`：
  重试耗尽与超时给出不同的 `FailureReason`；
- `TestRecoveryAfterInterruption`：第一批在任务执行中途取消，用新的
  `FileStore` 句柄模拟进程重启后恢复，已成功任务的副作用文件不重复产生；
- `TestDuplicateSubmissionDoesNotRerunSucceededTasks` /
  `TestConcurrentSameBatchSubmissionsAreSerialized`：重复 / 并发提交同一
  批次时任务体只执行一次；
- `TestConcurrencyCapQueuesTasks` / `TestConcurrentRaceUnderLoad`：并发
  上限不被突破、50 个任务高负载下无竞态（`-race`）。

## 最小用法

```go
st, _ := store.NewFileStore("./.state")
o, err := orchestrator.New(orchestrator.Config{
    BatchID:     "batch-001",
    Concurrency: 4,
    Retry:       retry.FixedPolicy{Attempts: 3, Delay: 100 * time.Millisecond},
    Store:       st,
    Logger:      orchestrator.NewTextLogger(os.Stdout),
    Tasks: []task.Task{
        {ID: "a", Run: fetchConfig},
        {ID: "b", Deps: []string{"a"}, Timeout: 5 * time.Second, Run: callAPI},
    },
})
if err != nil { /* 非法依赖图，启动期拒绝 */ }
snap, err := o.Run(context.Background())
// 终态与失败原因从 snap.States[id] 读取；任务失败本身不体现为 Run 的 error。
```
