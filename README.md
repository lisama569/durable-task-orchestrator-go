# Durable Task Orchestrator (Go)

一个轻量、可持久化的任务编排引擎：按 DAG 依赖并发调度一批作业，并在**失败重试、
进程中断重启、重复提交、取消/超时**等场景下给出稳定、可预期、可区分的结果。

## 能力概览

- **依赖调度**：无依赖的作业并发执行；依赖未成功完成的作业不会被提前启动。
- **启动期校验**：循环依赖、引用不存在的作业、空 ID、重复 ID 都在执行任何作业之前
  被拒绝，错误中明确指出是哪个作业、哪条依赖导致。
- **重试与超时**：每个作业可配置单次执行超时与指数退避重试；重试次数用尽后作业进入
  `failed`，失败原因是可区分的 `retries_exhausted`，并记录最后一次尝试的直接原因
  （`job_error` / `timeout` / `canceled`）。
- **幂等恢复**：以 `BatchID` 作为幂等键，状态以 JSON 原子落盘。
  - 已 `succeeded` 的作业在重启或重复提交时**绝不再次执行**，不重复产生副作用；
  - 进程中断残留的 `running` 状态会在恢复时重置为 `pending` 重新调度；
  - 上次失败/取消/跳过的作业，重新提交时重新排队尝试；
  - 同一 `BatchID` 的并发重复提交会被直接拒绝（同一时刻只允许一个编排器运行）。
- **并发上限与排队**：到达 `MaxConcurrency` 后，其余就绪作业保持 `pending` 排队，
  运行中的 goroutine 数量被严格限制在上限内，不会为排队作业堆积资源。
- **取消传播**：批次 `context` 取消或超时时，信号传播到每个正在运行的作业；
  未启动的作业落为明确的 `canceled` 终态，终态同样持久化，不留状态不明确的中间记录。

## 作业状态机

| 状态 | 含义 |
| --- | --- |
| `pending` | 已校验，等待依赖完成或等待并发槽位 |
| `running` | 作业函数正在执行（中断重启时会被判定为残留并重置） |
| `succeeded` | 成功完成，结果已持久化，**永不再执行** |
| `failed` | 不可重试失败或重试次数用尽 |
| `canceled` | 批次取消/超时，作业未取得成功结果 |
| `skipped` | 某个上游最终失败/取消，本作业不再被调度 |

失败原因（`FailureCode`）用于区分终态成因：

- `job_error`：作业函数返回错误或 panic；
- `timeout`：作业超过自身的 `AttemptTimeout`；
- `retries_exhausted`：重试次数用尽（此时 `LastFailure` 给出最后一次直接原因）；
- `canceled`：批次/作业被取消；
- `upstream_failed`：因上游终态失败而被跳过。

## 调度规则

1. 提交时先做 DAG 校验，任意结构问题都在执行前整体拒绝，返回 `ValidationError`
   列表，逐条说明 `kind`（`unknown_dependency` / `cycle` / `duplicate_job` /
   `empty_id`）、涉事作业与依赖。
2. 每轮调度计算就绪集合：**所有依赖均已 `succeeded`** 且自身为 `pending`。
   就绪集合按 ID 排序，保证调度顺序确定、可观测。
3. 就绪作业在并发上限内派发；槽位不足的作业保持 `pending`，等运行中作业归还槽位后
   由事件唤醒重新评估（不是忙轮询）。
4. 上游进入 `failed` / `canceled` / `skipped` 后，等待它的下游作业标记为
   `skipped`（`upstream_failed`），但与失败链路无关的作业继续正常执行。
5. 作业每次“派发 / 开始尝试 / 到达终态”都会立即持久化（先在锁内做状态快照，
   再原子写文件），崩溃后可从最近一次终态恢复。

## 重试与恢复语义

- 重试策略 `RetryPolicy{MaxAttempts, InitialBackoff, MaxBackoff, Multiplier}`，
  采用指数退避 `next = min(prev*Multiplier, MaxBackoff)`；退避期间也响应取消。
- 作业的单次执行受自身 `AttemptTimeout` 约束；超时计为一次失败尝试并参与重试。
- 当“作业刚好返回结果”与“取消/超时”同时发生时，优先采用作业的**明确结果**，
  避免“副作用实际已完成却被记为取消”，从而在重启后错误地重复执行。
- 状态文件写入采用“临时文件 + fsync + 原子 rename”，不会出现半个状态文件。
- 恢复规则：
  - `succeeded` → 保持，跳过执行（幂等核心保证）；
  - `running` → 视为崩溃残留，重置为 `pending`；
  - `failed` / `canceled` / `skipped` → 重新提交时重置为 `pending` 再试一次；
  - 历史状态里存在但本次定义已删除的作业：保留快照，不参与调度。

> 注意：引擎保证“成功的作业不被重复执行”，但单个作业函数在被取消/超时中断时是否
> 产生了半截外部副作用，取决于作业自身。需要强一次语义时，作业应结合
> `BatchID + JobID` 做外部幂等（如去重键、唯一约束）。

## 快速开始

```go
engine := orchestrator.NewEngine(
    orchestrator.NewFileStore("./state"),
    orchestrator.Config{
        MaxConcurrency: 4,
        DefaultRetry: orchestrator.RetryPolicy{
            MaxAttempts: 3, InitialBackoff: 50 * time.Millisecond,
            MaxBackoff: time.Second, Multiplier: 2,
        },
    },
)

result, err := engine.Run(ctx, "batch-2026-09-29", []orchestrator.Job{
    {ID: "a", Fn: func(ctx context.Context) error { return doA(ctx) }},
    {ID: "b", Deps: []orchestrator.JobID{"a"}, AttemptTimeout: 2 * time.Second,
        Fn: func(ctx context.Context) error { return doB(ctx) }},
})
```

## 本地验证

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全量测试（建议带 -race，覆盖循环依赖、重试耗尽、中断恢复、并发竞争）
go test ./... -race

# 覆盖率
go test ./... -cover

# 端到端演示：首次执行（含一次失败后重试成功）
go run ./cmd/demo -state-dir=./.demo-state
# 再次运行：已成功作业被幂等跳过
go run ./cmd/demo -state-dir=./.demo-state
# 演示重试耗尽与下游跳过（失败原因可区分）
go run ./cmd/demo -state-dir=./.demo-fail -fail-forever
```

运行后可直接查看状态文件，例如 `.demo-state/batch-demo-pipeline.json`，
其中包含每个作业的 `status`、`attempts`、`failure_code`、`last_failure` 与时间戳。

## 测试覆盖的关键场景

- 循环依赖（自环、多节点环）、未知依赖、空/重复 ID：启动期拒绝且零作业执行；
- 重试耗尽：状态为 `failed/retries_exhausted`，`LastFailure` 区分 `job_error`
  与 `timeout`；先失败后重试成功；
- 中断恢复：手工构造崩溃后的 `running` 残留，重启后正确重置并跑完，成功作业不重放；
- 重复提交：同一 `BatchID` 成功作业不重复执行、失败作业继续推进；并发重复提交被拒绝；
- 取消后恢复：运行中作业收到取消信号落为 `canceled`，重新提交后只重跑未成功部分；
- 并发竞争：并发峰值不超过上限、排队不堆积 goroutine，`-race` 下多轮稳定通过。

## 目录结构

```
internal/orchestrator/
  types.go    # 状态、失败码、重试策略等核心类型
  graph.go    # DAG 校验（未知依赖 / 环 / 重复 ID）
  store.go    # 持久化接口与原子 JSON 文件实现
  retry.go    # 重试策略归一化与指数退避
  engine.go   # 调度主循环、并发控制、取消传播、恢复与终态持久化
cmd/demo/     # 端到端示例
```
