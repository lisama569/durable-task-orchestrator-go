// Command demo 用一个小型示例批次演示任务编排：依赖调度、失败重试与中断恢复。
//
// 用法：
//
//	go run ./cmd/demo                 # 正常跑完一个含重试的 DAG
//	go run ./cmd/demo -state-dir=tmp  # 指定状态目录（可观察持久化 JSON）
//
// 状态默认写入 ./.demo-state，可在运行后查看 batch-demo-*.json。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/lisama569/durable-task-orchestrator-go/internal/orchestrator"
)

func main() {
	stateDir := flag.String("state-dir", "./.demo-state", "批次状态持久化目录")
	failForever := flag.Bool("fail-forever", false, "让 build 作业一直失败，用于演示重试耗尽")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	store := orchestrator.NewFileStore(*stateDir)
	engine := orchestrator.NewEngine(store, orchestrator.Config{
		MaxConcurrency: 2,
		DefaultRetry: orchestrator.RetryPolicy{
			MaxAttempts:    3,
			InitialBackoff: 50 * time.Millisecond,
			MaxBackoff:     200 * time.Millisecond,
			Multiplier:     2,
		},
		Logger: logger,
	})

	var testAttempts atomic.Int32
	jobs := []orchestrator.Job{
		{
			ID: "fetch-config",
			Fn: func(ctx context.Context) error {
				logger.Info("-> 执行 fetch-config")
				return nil
			},
		},
		{
			ID:   "run-tests",
			Deps: []orchestrator.JobID{"fetch-config"},
			Fn: func(ctx context.Context) error {
				n := testAttempts.Add(1)
				logger.Info("-> 执行 run-tests", "attempt", n)
				if n < 2 {
					return errors.New("模拟瞬时失败：依赖的测试服务尚未就绪")
				}
				return nil
			},
		},
		{
			ID:             "build",
			Deps:           []orchestrator.JobID{"run-tests"},
			AttemptTimeout: 2 * time.Second,
			Fn: func(ctx context.Context) error {
				logger.Info("-> 执行 build")
				if *failForever {
					return errors.New("模拟构建必现失败")
				}
				return nil
			},
		},
		{
			ID:   "publish",
			Deps: []orchestrator.JobID{"build"},
			Fn: func(ctx context.Context) error {
				logger.Info("-> 执行 publish")
				return nil
			},
		},
	}

	// 使用固定 BatchID：再次运行本程序时，已成功的作业不会重复执行，
	// 失败的作业会基于持久化状态继续重试，这就是“中断/重复提交恢复”。
	result, err := engine.Run(context.Background(), "demo-pipeline", jobs)
	if err != nil {
		logger.Error("批次被拒绝或引擎出错", "err", err)
		os.Exit(1)
	}

	fmt.Println()
	logger.Info("==== 批次结果汇总 ====", "batch_id", result.BatchID, "all_succeeded", result.AllSucceeded)
	for _, f := range result.Failed {
		logger.Warn("未成功作业",
			"job", f.ID, "status", f.Status,
			"failure_code", f.FailureCode,
			"last_failure", f.LastFailure,
			"attempts", f.Attempts,
			"message", f.FailureMsg)
	}
	if !result.AllSucceeded {
		os.Exit(2)
	}
	logger.Info("全部作业成功；再次运行本程序可验证已完成作业被幂等跳过")
}
