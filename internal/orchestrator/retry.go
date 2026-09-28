package orchestrator

import "time"

// normalizeRetry 补全重试策略中的零值/异常值，保证后续计算确定性。
func normalizeRetry(p RetryPolicy) RetryPolicy {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.Multiplier <= 1 {
		p.Multiplier = 2
	}
	if p.InitialBackoff < 0 {
		p.InitialBackoff = 0
	}
	if p.MaxBackoff <= 0 {
		// 未设置上限时给一个温和的默认上限，避免长重试链无限增长。
		p.MaxBackoff = 30 * time.Second
	}
	if p.MaxBackoff < p.InitialBackoff {
		p.MaxBackoff = p.InitialBackoff
	}
	return p
}

// backoff 返回第 attempt 次失败后、下一次尝试前应等待的时长。
// attempt 从 1 开始（即第 1 次执行失败后，第 2 次执行前的退避）。
func backoff(p RetryPolicy, attempt int) time.Duration {
	if attempt < 1 {
		return 0
	}
	d := p.InitialBackoff
	for i := 1; i < attempt; i++ {
		next := time.Duration(float64(d) * p.Multiplier)
		if next <= d || next > p.MaxBackoff { // 防溢出 + 钳到上限
			d = p.MaxBackoff
			break
		}
		d = next
	}
	if d > p.MaxBackoff {
		d = p.MaxBackoff
	}
	return d
}
