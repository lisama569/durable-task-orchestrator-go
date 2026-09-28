package orchestrator

import "fmt"

// ValidateDAG 在启动阶段校验作业定义：
//   - 作业 ID 非空且不重复；
//   - 每个依赖都指向一个真实存在的作业；
//   - 依赖图无环。
//
// 返回的每一条 ValidationError 都指出具体是哪一个作业、哪一条依赖导致问题。
// 只要返回值非空，调用方就必须拒绝整批提交，不启动任何作业。
func ValidateDAG(jobs []Job) []ValidationError {
	var errs []ValidationError

	known := make(map[JobID]bool, len(jobs))
	deps := make(map[JobID][]JobID, len(jobs))
	for _, j := range jobs {
		if j.ID == "" {
			errs = append(errs, ValidationError{
				Kind:    "empty_id",
				Message: "发现 ID 为空的作业，作业必须具有非空唯一标识",
			})
			continue
		}
		if known[j.ID] {
			errs = append(errs, ValidationError{
				Kind:    "duplicate_job",
				JobID:   j.ID,
				Message: fmt.Sprintf("作业 %q 重复定义，作业 ID 必须唯一", j.ID),
			})
			continue
		}
		known[j.ID] = true
		deps[j.ID] = append(deps[j.ID], j.Deps...)
	}

	// 引用未知作业的依赖（去重，避免同一坏边刷屏）。
	seenBadEdge := make(map[[2]JobID]bool)
	for _, j := range jobs {
		if j.ID == "" {
			continue
		}
		for _, dep := range j.Deps {
			key := [2]JobID{j.ID, dep}
			if seenBadEdge[key] {
				continue
			}
			seenBadEdge[key] = true
			if !known[dep] {
				errs = append(errs, ValidationError{
					Kind:  "unknown_dependency",
					JobID: j.ID,
					DepID: dep,
					Message: fmt.Sprintf(
						"作业 %q 引用了不存在的依赖 %q", j.ID, dep),
				})
			}
		}
	}

	// 环检测：DFS 三色标记。仅在无坏边时进行，避免因缺节点产生误报。
	const (
		white = 0 // 未访问
		gray  = 1 // 在当前 DFS 路径上
		black = 2 // 已完成
	)
	color := make(map[JobID]int, len(known))
	var stack []JobID
	var dfs func(id JobID)
	dfs = func(id JobID) {
		color[id] = gray
		stack = append(stack, id)
		for _, dep := range deps[id] {
			if !known[dep] {
				continue // 未知依赖已在上面单独报告
			}
			switch color[dep] {
			case white:
				dfs(dep)
			case gray:
				// dep 已在当前递归栈中：id -> dep 是闭环边。
				cycle := appendCycle(stack, dep)
				errs = append(errs, ValidationError{
					Kind:  "cycle",
					JobID: id,
					DepID: dep,
					Message: fmt.Sprintf(
						"检测到循环依赖：作业 %q 依赖 %q，闭环路径为 %s",
						id, dep, formatCycle(cycle)),
				})
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
	}
	for _, j := range jobs {
		if j.ID == "" || color[j.ID] != white {
			continue
		}
		dfs(j.ID)
	}

	return errs
}

// appendCycle 从 DFS 栈中截出从 dep 回到 dep 的闭环节点序列。
func appendCycle(stack []JobID, dep JobID) []JobID {
	for i, id := range stack {
		if id == dep {
			cyc := make([]JobID, 0, len(stack)-i+1)
			cyc = append(cyc, stack[i:]...)
			cyc = append(cyc, dep)
			return cyc
		}
	}
	return []JobID{dep, dep}
}

func formatCycle(cycle []JobID) string {
	s := ""
	for i, id := range cycle {
		if i > 0 {
			s += " -> "
		}
		s += string(id)
	}
	return s
}
