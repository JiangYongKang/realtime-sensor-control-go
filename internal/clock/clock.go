// Package clock 提供可手动推进的时钟，用于确定性调度与测试。
package clock

import (
	"sync"
	"time"
)

// Clock 抽象时间源。
type Clock interface {
	Now() time.Time
}

// Manual 是可手动推进的时钟，Advance 单调递增，保证确定性。
type Manual struct {
	mu  sync.Mutex
	now time.Time
}

// NewManual 创建从 start 开始的手动时钟。
func NewManual(start time.Time) *Manual {
	return &Manual{now: start}
}

// Now 返回当前时间。
func (c *Manual) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance 将时钟推进 d，返回推进后的时间。d 必须非负。
func (c *Manual) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
	return c.now
}
