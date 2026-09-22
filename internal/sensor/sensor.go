// Package sensor 提供模拟传感器设备。
package sensor

import (
	"math"
	"time"
)

// Sample 表示一次采样结果。
type Sample struct {
	ChannelID string
	Seq       uint64
	Value     float64
	Scheduled time.Time // 计划采样时刻
	Acquired  time.Time // 实际采样时刻
}

// Device 是传感器设备接口。
type Device interface {
	// Read 在时刻 now 对通道 channelID 采样。
	Read(channelID string, now time.Time) (float64, error)
}

// SimDevice 是确定性的模拟设备：value = base + amp*sin(2*pi*t/period)。
// 同一输入时刻永远产生同一输出，保证测试可复现。
type SimDevice struct {
	Base   float64
	Amp    float64
	Period time.Duration
}

// Read 实现 Device。
func (d *SimDevice) Read(channelID string, now time.Time) (float64, error) {
	phase := 2 * math.Pi * float64(now.UnixNano()%d.Period.Nanoseconds()) / float64(d.Period.Nanoseconds())
	return d.Base + d.Amp*math.Sin(phase), nil
}
