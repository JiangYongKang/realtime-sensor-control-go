// Package system 编排采样、协议与安全控制。
//
// 时间语义：外部通过 Advance(t) 单调推进手动时钟；每次 Advance 处理
// (上次时刻, t] 内所有到期采样。采样在计划时刻到期，若处理时刻 t 超过
// Scheduled+Deadline 则判定为迟到：迟到样本计入看门狗与统计，但不参与
// 控制输出，不会被静默丢弃，也不会被提前消费。
package system

import (
	"sync"
	"time"

	"github.com/highcumontoa/realtime-sensor-control-go/internal/clock"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/protocol"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/safety"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/scheduler"
	"github.com/highcumontoa/realtime-sensor-control-go/internal/sensor"
)

// ChannelConfig 描述一路采样通道。
type ChannelConfig struct {
	Task      scheduler.Task
	SafeValue float64 // 安全状态下的控制输出
	MaxMisses int     // 连续错过截止时间的看门狗阈值
	Timeout   time.Duration
}

// ChannelStatus 是通道状态快照。
type ChannelStatus struct {
	State        string
	ConsecMisses int
	LateTotal    uint64
	LastValue    float64
	LastSeq      uint64
	Output       float64
}

// Snapshot 是系统状态的一致性快照。
type Snapshot struct {
	Now      time.Time
	Channels map[string]ChannelStatus
}

type channelState struct {
	cfg       ChannelConfig
	wd        *safety.Watchdog
	lastValue float64
	lastSeq   uint64
	lateTotal uint64
	output    float64
}

// System 是采样与安全控制主体，所有导出方法并发安全。
type System struct {
	mu     sync.RWMutex
	clock  *clock.Manual
	device sensor.Device
	sched  *scheduler.Scheduler
	chans  map[string]*channelState
	dec    *protocol.Decoder
}

// New 创建系统。
func New(clk *clock.Manual, dev sensor.Device, chans []ChannelConfig) *System {
	s := &System{
		clock:  clk,
		device: dev,
		chans:  map[string]*channelState{},
		dec:    protocol.NewDecoder(protocol.TagSeq, protocol.TagValue, protocol.TagChannel),
	}
	tasks := make([]scheduler.Task, 0, len(chans))
	for _, c := range chans {
		s.chans[c.Task.ID] = &channelState{
			cfg: c,
			wd: &safety.Watchdog{
				MaxMisses: c.MaxMisses,
				Timeout:   c.Timeout,
			},
			output: c.SafeValue, // 启动即处于安全输出，直到首个按时样本
		}
		tasks = append(tasks, c.Task)
	}
	s.sched = scheduler.New(clk.Now(), tasks)
	return s
}

// Advance 将时钟推进到 t 并处理期间所有到期采样与控制。
// t 必须不早于当前时刻。
func (s *System) Advance(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Advance(t.Sub(s.clock.Now()))
	for _, due := range s.sched.DueAt(now) {
		s.process(due, now)
	}
	// 静默超时检查：整个窗口没有任何采样事件的通道也要触发看门狗。
	for _, ch := range s.chans {
		prev := ch.wd.State()
		if ch.wd.Tick(now) == safety.StateSafe && prev != safety.StateSafe {
			s.enterSafe(ch)
		}
	}
}

// process 处理一次到期采样（调用方须持写锁）。
func (s *System) process(due scheduler.Due, now time.Time) {
	ch := s.chans[due.Task.ID]
	onTime := !now.After(due.Scheduled.Add(due.Task.Deadline))
	prev := ch.wd.State()
	st := ch.wd.Update(safety.Event{At: now, OnTime: onTime})
	if !onTime {
		// 迟到样本：计入看门狗与统计，但不更新控制输出。
		ch.lateTotal++
	}
	if st == safety.StateSafe {
		if prev != safety.StateSafe {
			s.enterSafe(ch)
		}
		return
	}
	if onTime {
		v, err := s.device.Read(due.Task.ID, due.Scheduled)
		if err != nil {
			ch.lateTotal++ // 读取失败按一次异常处理，不更新输出
			return
		}
		ch.lastValue, ch.lastSeq = v, due.Seq
		ch.output = v
	}
}

// enterSafe 进入安全状态：输出切换为安全值，并丢弃故障前状态，
// 保证故障恢复后不沿用旧值（调用方须持写锁）。
func (s *System) enterSafe(ch *channelState) {
	ch.output = ch.cfg.SafeValue
	ch.lastValue, ch.lastSeq = 0, 0
}

// FeedBus 处理总线字节流，返回解码报文与逐帧错误。
func (s *System) FeedBus(data []byte) ([]protocol.Message, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dec.Feed(data)
}

// FinishBus 结束总线输入，残留不完整帧时返回 ErrTruncated。
func (s *System) FinishBus() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dec.Finish()
}

// Snapshot 返回系统状态一致性快照。
func (s *System) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{Now: s.clock.Now(), Channels: map[string]ChannelStatus{}}
	for id, ch := range s.chans {
		snap.Channels[id] = ChannelStatus{
			State:        ch.wd.State().String(),
			ConsecMisses: ch.wd.Misses(),
			LateTotal:    ch.lateTotal,
			LastValue:    ch.lastValue,
			LastSeq:      ch.lastSeq,
			Output:       ch.output,
		}
	}
	return snap
}
