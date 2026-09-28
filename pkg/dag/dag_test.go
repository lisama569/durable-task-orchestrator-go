package dag

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestValidateOK(t *testing.T) {
	ids := []string{"a", "b", "c", "d"}
	deps := map[string][]string{
		"a": nil,
		"b": {"a"},
		"c": {"a"},
		"d": {"b", "c"},
	}
	if err := Validate(ids, deps); err != nil {
		t.Fatalf("expected valid graph, got %v", err)
	}
	order, err := TopoOrder(ids, deps)
	if err != nil {
		t.Fatalf("topo: %v", err)
	}
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	if pos["a"] >= pos["b"] || pos["a"] >= pos["c"] || pos["b"] >= pos["d"] || pos["c"] >= pos["d"] {
		t.Fatalf("ordering violates dependencies: %v", order)
	}
}

func TestValidateEmptyID(t *testing.T) {
	err := Validate([]string{"a", ""}, nil)
	if !errors.Is(err, ErrEmptyID) {
		t.Fatalf("want ErrEmptyID, got %v", err)
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("error should explain the cause: %v", err)
	}
}

func TestValidateDuplicateID(t *testing.T) {
	err := Validate([]string{"a", "a"}, nil)
	if !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("want ErrDuplicateID, got %v", err)
	}
	if !strings.Contains(err.Error(), `"a"`) {
		t.Fatalf("error should name the duplicated id: %v", err)
	}
}

func TestValidateMissingDependency(t *testing.T) {
	err := Validate([]string{"a", "b"}, map[string][]string{"a": {"ghost"}, "b": nil})
	if !errors.Is(err, ErrMissingDep) {
		t.Fatalf("want ErrMissingDep, got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, `"a"`) || !strings.Contains(msg, `"ghost"`) {
		t.Fatalf("error must identify the task edge (a -> ghost): %v", err)
	}
}

func TestValidateSimpleCycle(t *testing.T) {
	ids := []string{"a", "b", "c"}
	deps := map[string][]string{
		"a": {"b"},
		"b": {"c"},
		"c": {"a"},
	}
	err := Validate(ids, deps)
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("want ErrCycle, got %v", err)
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("error should show the cycle path: %v", err)
	}
	// All three nodes of the cycle should appear in the detail.
	for _, id := range []string{"a", "b", "c"} {
		if !strings.Contains(err.Error(), id) {
			t.Fatalf("cycle detail missing node %s: %v", id, err)
		}
	}
}

func TestValidateSelfCycle(t *testing.T) {
	err := Validate([]string{"a"}, map[string][]string{"a": {"a"}})
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("want ErrCycle for self dependency, got %v", err)
	}
}

func TestValidateDiamondAcyclic(t *testing.T) {
	ids := []string{"root", "l", "r", "leaf"}
	deps := map[string][]string{
		"root": nil,
		"l":    {"root"},
		"r":    {"root"},
		"leaf": {"l", "r"},
	}
	order, err := TopoOrder(ids, deps)
	if err != nil {
		t.Fatalf("diamond graph must be acyclic: %v", err)
	}
	if !reflect.DeepEqual(len(order), 4) {
		t.Fatalf("want 4 nodes, got %d", len(order))
	}
}

func TestTopoOrderDeterministic(t *testing.T) {
	ids := []string{"a", "b", "c"}
	deps := map[string][]string{"a": nil, "b": nil, "c": nil}
	first, err := TopoOrder(ids, deps)
	if err != nil {
		t.Fatal(err)
	}
	second, err := TopoOrder(ids, deps)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("topo order not deterministic: %v vs %v", first, second)
	}
}
