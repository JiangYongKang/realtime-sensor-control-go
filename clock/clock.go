// Package clock 提供可手工推进的确定时钟，用于调度与测试。
package clock

import "sync/atomic"

// Clock 是单调时钟抽象，时间以纳秒计。
type Clock interface {
	// Now 返回当前单调时间（纳秒）。
	Now() int64
}

// ManualClock 是只能显式推进的时钟，保证测试与调度完全确定。
// 其方法可并发调用。
type ManualClock struct {
	now atomic.Int64
}

// NewManual 创建起始时间为 start 的手工时钟。
func NewManual(start int64) *ManualClock {
	c := &ManualClock{}
	c.now.Store(start)
	return c
}

// Now 返回当前时间。
func (c *ManualClock) Now() int64 { return c.now.Load() }

// Advance 将时钟推进 d 纳秒，d 必须非负。
func (c *ManualClock) Advance(d int64) {
	if d < 0 {
		panic("clock: negative advance")
	}
	c.now.Add(d)
}
