// Package system 编排采样、协议与安全控制。
//
// 时间语义：外部通过 Advance(t) 单调推进手动时钟；每次 Advance 处理
// (上次时刻, t] 内所有到期采样。采样在计划时刻到期，若处理时刻 t 超过
// Scheduled+Deadline 则判定为迟到：迟到样本计入看门狗与统计，但不参与
// 控制输出，不会被静默丢弃，也不会被提前消费。
package system

import (
	"math"
	"sync"
	"time"

	"github.com/highcumontoa/realtime-sensor-control-go/internal/busguard"
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
	// Bus 配置该通道的总线报文安全判定；零值字段填充为 busguard 默认值。
	Bus busguard.Config
}

// ChannelStatus 是通道状态快照。
type ChannelStatus struct {
	State        string
	ConsecMisses int
	LateTotal    uint64
	LastValue    float64
	LastSeq      uint64
	Output       float64
	// BusSafe 表示总线报文安全判定是否已降级；State 同时综合采样看门狗。
	BusSafe bool
	// BadStreak 是当前连续不可接受总线报文帧数。
	BadStreak int
	// BadTotal 是累计不可接受总线报文帧数。
	BadTotal uint64
	// ReorderTotal 是容忍窗口内乱序报文数（不计数、不采用）。
	ReorderTotal uint64
}

// Snapshot 是系统状态的一致性快照。
type Snapshot struct {
	Now      time.Time
	Channels map[string]ChannelStatus
	// UnroutableTotal 是无法归属到任何配置通道的报文/错误总数。
	UnroutableTotal uint64
}

type channelState struct {
	cfg       ChannelConfig
	wd        *safety.Watchdog
	guard     *busguard.Guard
	lastValue float64
	lastSeq   uint64
	lateTotal uint64
	output    float64
}

// System 是采样与安全控制主体，所有导出方法并发安全。
type System struct {
	mu            sync.RWMutex
	clock         *clock.Manual
	device        sensor.Device
	sched         *scheduler.Scheduler
	chans         map[string]*channelState
	dec           *protocol.Decoder
	unroutableTot uint64
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
			guard:  busguard.New(c.Bus),
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
	for _, ch := range s.chans {
		// 采样看门狗：静默超时。
		wdPrev := ch.wd.State()
		if ch.wd.Tick(now) == safety.StateSafe && wdPrev != safety.StateSafe {
			s.enterSafe(ch)
			continue
		}
		// 总线安全判定：异常持续时间窗口（不依赖采样超时兜底）。
		wasSafe := ch.guard.Status().Safe
		ch.guard.Tick(now)
		if !wasSafe && ch.guard.Status().Safe {
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
	if onTime && !ch.guard.Status().Safe {
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
//
// 报文按 TagChannel 归属到具体通道：属于某通道的帧（无论好坏）参与该
// 通道的总线安全判定；连续不可接受报文达到帧数阈值或时间窗口，即在
// 本次调用内切到安全输出，不等待采样超时兜底。无法归属（缺通道字段、
// 通道名非法/未知、TLV 结构损坏）的帧计入 UnroutableTotal，不影响任何
// 已配置通道。整批处理在同一把写锁内完成，快照不会看到半批更新。
func (s *System) FeedBus(data []byte) ([]protocol.Message, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs, errs := []protocol.Message{}, []error{}
	// 按字节流中的真实顺序逐帧处理，保证好/坏帧穿插时计数确定。
	for _, ev := range s.dec.FeedEvents(data) {
		if ev.Err != nil {
			errs = append(errs, ev.Err)
			s.routeFault(ev.Err)
			continue
		}
		if err := s.routeMessage(ev.Msg); err != nil {
			errs = append(errs, err)
		} else {
			msgs = append(msgs, ev.Msg)
		}
	}
	return msgs, errs
}

// routeFault 把一帧解码错误归属到其声明通道（尽力判定），
// 无法可靠归属则计入全局 UnroutableTotal（调用方须持写锁）。
func (s *System) routeFault(err error) {
	fe, ok := err.(*protocol.FrameError)
	if !ok || fe.Channel == "" {
		s.unroutableTot++
		return
	}
	ch := s.chans[fe.Channel]
	if ch == nil {
		s.unroutableTot++
		return
	}
	s.noteBusFault(ch)
}

// routeMessage 校验并把一帧成功解码的报文归属、应用到通道
// （调用方须持写锁）。语义非法（缺通道/序号/值、值为 NaN、
// 类型未知）同样按一次不可接受报文处理。
func (s *System) routeMessage(m protocol.Message) error {
	rawCh, ok := m.Fields[protocol.TagChannel]
	if !ok || len(rawCh) == 0 {
		s.unroutableTot++
		return &protocol.FrameError{Err: protocol.ErrInvalidField}
	}
	chID := string(rawCh)
	ch := s.chans[chID]
	if ch == nil {
		s.unroutableTot++
		return &protocol.FrameError{Err: protocol.ErrInvalidField, Channel: chID}
	}
	seq, err := protocol.GetU64(m, protocol.TagSeq)
	if err != nil {
		s.noteBusFault(ch)
		return &protocol.FrameError{Err: err, Channel: chID}
	}
	v, err := protocol.GetF64(m, protocol.TagValue)
	if err != nil || math.IsNaN(v) {
		s.noteBusFault(ch)
		return &protocol.FrameError{Err: protocol.ErrInvalidField, Channel: chID}
	}
	if m.Type != protocol.TypeSample && m.Type != protocol.TypeControl {
		s.noteBusFault(ch)
		return &protocol.FrameError{Err: protocol.ErrInvalidField, Channel: chID}
	}
	now := s.clock.Now()
	wasSafe := ch.guard.Status().Safe
	disp := ch.guard.Observe(now, seq)
	switch disp {
	case busguard.DispBad:
		// 序号重复或明显倒退：Observe 已计入异常，检查是否触发降级。
		if !wasSafe && ch.guard.Status().Safe {
			s.enterSafe(ch)
		}
		return &protocol.FrameError{Err: protocol.ErrInvalidField, Channel: chID}
	case busguard.DispReordered:
		// 容忍窗口内乱序：不计数、不采用，不计为错误失败。
		return nil
	case busguard.DispStale:
		// SAFE 期间的故障前序号报文：丢弃，不得恢复。
		return nil
	default: // DispApplied
		// 仅当采样看门狗也健康时才更新控制输出；否则只推进总线序号基线，
		// 等采样侧恢复后输出仍来自恢复之后的新报文。
		if ch.wd.State() == safety.StateSafe {
			return nil
		}
		ch.lastValue, ch.lastSeq, ch.output = v, seq, v
		return nil
	}
}

// noteBusFault 记录某通道一帧不可接受报文，并在触发阈值时切入安全输出
// （调用方须持写锁）。
func (s *System) noteBusFault(ch *channelState) {
	wasSafe := ch.guard.Status().Safe
	ch.guard.Fault(s.clock.Now())
	if !wasSafe && ch.guard.Status().Safe {
		s.enterSafe(ch)
	}
}

// FinishBus 结束总线输入，残留不完整帧时返回 ErrTruncated。
func (s *System) FinishBus() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dec.Finish()
}

// Snapshot 返回系统状态一致性快照。采样看门狗与总线安全判定任一失效，
// 通道状态即为 SAFE；整张快照在同一把读锁下拷贝，不会看到半更新。
func (s *System) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{Now: s.clock.Now(), Channels: map[string]ChannelStatus{}, UnroutableTotal: s.unroutableTot}
	for id, ch := range s.chans {
		gs := ch.guard.Status()
		state := ch.wd.State()
		if gs.Safe {
			state = safety.StateSafe
		}
		snap.Channels[id] = ChannelStatus{
			State:        state.String(),
			ConsecMisses: ch.wd.Misses(),
			LateTotal:    ch.lateTotal,
			LastValue:    ch.lastValue,
			LastSeq:      ch.lastSeq,
			Output:       ch.output,
			BusSafe:      gs.Safe,
			BadStreak:    gs.BadStreak,
			BadTotal:     gs.BadTotal,
			ReorderTotal: gs.ReorderTotal,
		}
	}
	return snap
}
