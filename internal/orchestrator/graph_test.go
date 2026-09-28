package orchestrator

import (
	"context"
	"testing"
)

func noop(ctx context.Context) error { return nil }

func TestValidateDAG_UnknownDependency(t *testing.T) {
	jobs := []Job{
		{ID: "a", Deps: []JobID{"ghost"}, Fn: noop},
		{ID: "b", Deps: []JobID{"a"}, Fn: noop},
	}
	errs := ValidateDAG(jobs)
	if len(errs) != 1 {
		t.Fatalf("期望 1 条校验错误，得到 %d: %+v", len(errs), errs)
	}
	if errs[0].Kind != "unknown_dependency" || errs[0].JobID != "a" || errs[0].DepID != "ghost" {
		t.Fatalf("错误定位不正确: %+v", errs[0])
	}
}

func TestValidateDAG_SelfCycle(t *testing.T) {
	jobs := []Job{{ID: "a", Deps: []JobID{"a"}, Fn: noop}}
	errs := ValidateDAG(jobs)
	if len(errs) != 1 || errs[0].Kind != "cycle" {
		t.Fatalf("期望 1 条 cycle 错误，得到 %+v", errs)
	}
	if errs[0].JobID != "a" || errs[0].DepID != "a" {
		t.Fatalf("环错误定位不正确: %+v", errs[0])
	}
}

func TestValidateDAG_MultiNodeCycle(t *testing.T) {
	jobs := []Job{
		{ID: "a", Deps: []JobID{"b"}, Fn: noop},
		{ID: "b", Deps: []JobID{"c"}, Fn: noop},
		{ID: "c", Deps: []JobID{"a"}, Fn: noop},
	}
	errs := ValidateDAG(jobs)
	if len(errs) == 0 {
		t.Fatal("期望检测到 a->b->c->a 的环")
	}
	for _, e := range errs {
		if e.Kind != "cycle" {
			t.Fatalf("期望全部为 cycle 错误，得到 %+v", e)
		}
	}
}

func TestValidateDAG_EmptyIDAndDuplicate(t *testing.T) {
	jobs := []Job{
		{ID: "", Fn: noop},
		{ID: "x", Fn: noop},
		{ID: "x", Fn: noop},
	}
	errs := ValidateDAG(jobs)
	kinds := map[string]int{}
	for _, e := range errs {
		kinds[e.Kind]++
	}
	if kinds["empty_id"] != 1 || kinds["duplicate_job"] != 1 {
		t.Fatalf("期望 empty_id 与 duplicate_job 各 1 条，得到 %+v", kinds)
	}
}

func TestValidateDAG_Valid(t *testing.T) {
	jobs := []Job{
		{ID: "a", Fn: noop},
		{ID: "b", Deps: []JobID{"a"}, Fn: noop},
		{ID: "c", Deps: []JobID{"a", "b"}, Fn: noop},
	}
	if errs := ValidateDAG(jobs); len(errs) != 0 {
		t.Fatalf("合法 DAG 不应有错误，得到 %+v", errs)
	}
}
