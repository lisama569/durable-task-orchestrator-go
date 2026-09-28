// Package dag validates task dependency graphs.
package dag

import (
	"errors"
	"fmt"
)

// ValidationError describes why the dependency graph was rejected at startup,
// pointing at the exact edge or node that caused the rejection.
type ValidationError struct {
	Cause  error
	Detail string
}

func (e *ValidationError) Error() string {
	if e.Detail == "" {
		return e.Cause.Error()
	}
	return e.Cause.Error() + ": " + e.Detail
}

func (e *ValidationError) Unwrap() error { return e.Cause }

var (
	// ErrCycle is returned when the dependency graph contains a cycle.
	ErrCycle = errors.New("dag: cyclic dependency detected")
	// ErrMissingDep is returned when a task depends on an undefined task.
	ErrMissingDep = errors.New("dag: dependency references missing task")
	// ErrDuplicateID is returned when the same task ID appears twice.
	ErrDuplicateID = errors.New("dag: duplicate task id")
	// ErrEmptyID is returned when a task has an empty ID.
	ErrEmptyID = errors.New("dag: empty task id")
)

// Validate checks the graph for empty/duplicate IDs, missing dependencies and
// cycles. deps maps a task ID to the IDs it depends on (edges point from a
// dependency to its dependents semantically; here they are stored as
// task -> its prerequisites).
func Validate(ids []string, deps map[string][]string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			return &ValidationError{Cause: ErrEmptyID, Detail: "task at index " + fmt.Sprint(len(seen)) + " has an empty id"}
		}
		if _, dup := seen[id]; dup {
			return &ValidationError{Cause: ErrDuplicateID, Detail: fmt.Sprintf("task id %q declared more than once", id)}
		}
		seen[id] = struct{}{}
	}

	for _, id := range ids {
		for _, dep := range deps[id] {
			if _, ok := seen[dep]; !ok {
				return &ValidationError{
					Cause:  ErrMissingDep,
					Detail: fmt.Sprintf("task %q depends on %q, which is not defined", id, dep),
				}
			}
			if dep == id {
				return &ValidationError{
					Cause:  ErrCycle,
					Detail: fmt.Sprintf("task %q depends on itself", id),
				}
			}
		}
	}

	if cycle := findCycle(ids, deps); cycle != nil {
		return &ValidationError{
			Cause:  ErrCycle,
			Detail: fmt.Sprintf("cycle: %v", cycle),
		}
	}
	return nil
}

// findCycle performs an iterative DFS and returns the node sequence of the
// first cycle found, or nil when the graph is acyclic.
func findCycle(ids []string, deps map[string][]string) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(ids))

	// frame tracks the DFS position for one node: node index and next edge.
	type frame struct {
		id  string
		pos int
	}

	for _, root := range ids {
		if color[root] != white {
			continue
		}
		stack := []frame{{id: root}}
		path := []string{}
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			edges := deps[top.id]
			if top.pos == 0 {
				color[top.id] = gray
				path = append(path, top.id)
			}
			if top.pos < len(edges) {
				next := edges[top.pos]
				top.pos++
				switch color[next] {
				case white:
					stack = append(stack, frame{id: next})
				case gray:
					// Found a back edge: extract the cycle from the path.
					start := 0
					for i, n := range path {
						if n == next {
							start = i
							break
						}
					}
					cycle := append([]string{}, path[start:]...)
					return append(cycle, next)
				}
				continue
			}
			color[top.id] = black
			stack = stack[:len(stack)-1]
			path = path[:len(path)-1]
		}
	}
	return nil
}

// TopoOrder returns a topological ordering in which every task appears after
// all of its dependencies. It validates the graph first.
func TopoOrder(ids []string, deps map[string][]string) ([]string, error) {
	if err := Validate(ids, deps); err != nil {
		return nil, err
	}
	indegree := make(map[string]int, len(ids))
	dependents := make(map[string][]string, len(ids))
	for _, id := range ids {
		indegree[id] = len(deps[id])
		for _, dep := range deps[id] {
			dependents[dep] = append(dependents[dep], id)
		}
	}
	// Kahn's algorithm; iterate ids in submission order for deterministic output.
	queue := make([]string, 0, len(ids))
	for _, id := range ids {
		if indegree[id] == 0 {
			queue = append(queue, id)
		}
	}
	order := make([]string, 0, len(ids))
	for len(queue) > 0 {
		// Pick the first ready node in submission order for determinism.
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		for _, dep := range dependents[id] {
			indegree[dep]--
			if indegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}
	if len(order) != len(ids) {
		return nil, &ValidationError{Cause: ErrCycle, Detail: "topological sort failed"}
	}
	return order, nil
}
