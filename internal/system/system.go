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

	"github.com/highcumontoa/realtime-sensor-control-go/internal/busfault"
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
	// Bus 为该通道的总线报文异常监控配置；零值取 busfault 默认值
	// （连续 3 帧无法接受的报文即降级、乱序容忍 0、1 帧有效报文恢复）。
	Bus busfault.Config
}

// ChannelStatus 是通道状态快照。
type ChannelStatus struct {
	State        string
	ConsecMisses int
	LateTotal    uint64
	LastValue    float64
	LastSeq      uint64
	Output       float64
	// BusState 为通道级总线监控状态（OK/SAFE）。
	BusState string
	// BusConsecFails 为总线连续异常帧数。
	BusConsecFails int
	BusAccepted    uint64
	BusRejected    uint64
	// BusLastSeq/BusLastValue 为最近一次被接受的总线报文序号与载荷值。
	BusLastSeq   uint64
	BusLastValue float64
}

// Snapshot 是系统状态的一致性快照。
type Snapshot struct {
	Now      time.Time
	Channels map[string]ChannelStatus
	// BusUnrouted 为复用总线（FeedBus）上无法归属到任何已配置通道的
	// 报文/错误计数：不含通道字段的合法帧、通道名不匹配帧与解码失败。
	BusUnrouted uint64
}

type channelState struct {
	cfg       ChannelConfig
	wd        *safety.Watchdog
	bus       *busfault.Monitor
	dec       *protocol.Decoder // 通道专用总线解码器（FeedChannelBus）
	decMu     sync.Mutex        // 仅保护解码；解码不持全局锁，避免长流拖累其他通道
	inSafe    bool              // 输出当前是否被任一故障源强制为安全值
	lastValue float64
	lastSeq   uint64
	lateTotal uint64
	output    float64
	busSeq    uint64
	busValue  float64
}

// System 是采样与安全控制主体，所有导出方法并发安全。
type System struct {
	mu       sync.RWMutex
	muxMu    sync.Mutex // 仅保护复用解码器
	clock    *clock.Manual
	device   sensor.Device
	sched    *scheduler.Scheduler
	chans    map[string]*channelState
	chanIDs  []string // 确定性快照顺序
	dec      *protocol.Decoder
	unrouted uint64
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
			bus:    busfault.New(c.Bus),
			dec:    protocol.NewDecoder(protocol.TagSeq, protocol.TagValue, protocol.TagChannel),
			output: c.SafeValue, // 启动即处于安全输出，直到首个按时样本
		}
		s.chanIDs = append(s.chanIDs, c.Task.ID)
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
	// 静默超时检查：整个窗口没有任何采样事件的通道也要触发看门狗；
	// 同时让总线异常时间窗随时间重评。
	for _, id := range s.chanIDs {
		ch := s.chans[id]
		prev := ch.wd.State()
		st := ch.wd.Tick(now)
		if st == safety.StateSafe && prev != safety.StateSafe {
			ch.bus.Reset() // 看门狗触发安全：故障前总线状态同样不得沿用
		}
		ch.bus.Tick(now)
		s.reconcile(ch)
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
	if st == safety.StateSafe && prev != safety.StateSafe {
		ch.bus.Reset()
	}
	if s.combinedState(ch) == safety.StateSafe {
		s.reconcile(ch)
		return
	}
	if onTime {
		v, err := s.device.Read(due.Task.ID, due.Scheduled)
		if err != nil {
			ch.lateTotal++ // 读取失败按一次异常处理，不更新输出
			s.reconcile(ch)
			return
		}
		ch.lastValue, ch.lastSeq = v, due.Seq
		ch.output = v
	}
	s.reconcile(ch)
}

// reconcile 汇总看门狗与通道级总线监控的当前状态，维护安全输出与
// “进入安全即丢弃故障前全部状态”的不变量。仅在状态实际翻转时清除，
// 恢复期间到达的新报文不会被反复抹掉（调用方须持写锁）。
func (s *System) reconcile(ch *channelState) {
	safe := s.combinedState(ch) == safety.StateSafe
	if safe && !ch.inSafe {
		ch.inSafe = true
		ch.output = ch.cfg.SafeValue
		ch.lastValue, ch.lastSeq = 0, 0
		ch.busSeq, ch.busValue = 0, 0
	}
	if !safe && ch.inSafe {
		ch.inSafe = false
	}
}

// FeedBus 处理复用总线字节流，返回解码报文与逐帧错误。
//
// 归属规则：解码成功且带合法 TagChannel 字段的帧按通道落到对应通道的
// 采样/控制与安全判定；其余（解码失败、无通道字段、通道名未配置）一律
// 计入 BusUnrouted，不拖垮任何健康通道。需要逐字节归因时使用
// FeedChannelBus。
func (s *System) FeedBus(data []byte) ([]protocol.Message, []error) {
	// 解码在全局状态锁之外完成：一路通道的长流式输入不会停住其他通道。
	s.muxMu.Lock()
	msgs, errs := s.dec.Feed(data)
	s.muxMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	for range errs {
		// 复用总线上解码失败的帧无法可靠读出通道归属，统一计入未归属。
		s.unrouted++
	}
	for _, m := range msgs {
		s.routeMuxMessage(m, now)
	}
	return msgs, errs
}

// routeMuxMessage 将复用总线上解码成功的帧按 TagChannel 归属
// （调用方须持写锁）。
func (s *System) routeMuxMessage(m protocol.Message, now time.Time) {
	cid, err := protocol.GetChannel(m)
	if err != nil {
		s.unrouted++ // 无合法通道字段：无法归属
		return
	}
	ch, ok := s.chans[cid]
	if !ok {
		s.unrouted++ // 通道名未配置：无法归属
		return
	}
	s.applyMessage(ch, m, now)
}

func (s *System) FeedChannelBus(id string, data []byte) ([]protocol.Message, []error) {
	ch, ok := s.chans[id]
	if !ok {
		return nil, nil
	}
	// 解码在通道自己的锁上完成，与其他通道完全隔离。
	ch.decMu.Lock()
	msgs, errs := ch.dec.Feed(data)
	ch.decMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	for range errs {
		ch.bus.Fail(now)
	}
	for _, m := range msgs {
		s.applyMessage(ch, m, now)
	}
	s.reconcile(ch)
	return msgs, errs
}

// FinishChannelBus 结束通道专用总线输入，残留不完整帧时返回 ErrTruncated
// 并将其计为该通道的一次异常。
func (s *System) FinishChannelBus(id string) error {
	ch, ok := s.chans[id]
	if !ok {
		return nil
	}
	ch.decMu.Lock()
	err := ch.dec.Finish()
	ch.decMu.Unlock()
	if err != nil {
		s.mu.Lock()
		ch.bus.Fail(s.clock.Now())
		s.reconcile(ch)
		s.mu.Unlock()
	}
	return err
}

// applyMessage 将一帧已完整解码的报文应用到指定通道（调用方须持写锁）。
//
// 处理规则：
//   - 带序号帧先经 busfault 判定：接受/乱序容忍/活性帧清零异常计数；
//     重复或倒退超容忍按一次异常处理。
//   - TypeControl 且带 TagValue：被接受的帧用其值更新控制输出；
//     TypeSample 且带 TagValue：更新该通道最近总线采样值（BusLastValue），
//     不覆盖调度采样链路上的输出。
//   - 乱序容忍帧（AcceptedReordered）只清零异常计数，不应用载荷，
//     避免迟到旧数据覆盖当前输出。
//   - 只要看门狗或总线监控任一处于 SAFE，所有载荷都不生效，输出保持
//     安全值；恢复后的首帧建立全新序号基线并重新开始。
func (s *System) applyMessage(ch *channelState, m protocol.Message, now time.Time) {
	var r busfault.Result
	seqVal, seqErr := protocol.GetU64(m, protocol.TagSeq)
	if seqErr == nil {
		r = ch.bus.Frame(now, seqVal)
	} else {
		r = ch.bus.Alive(now)
	}
	if r == busfault.Rejected {
		s.reconcile(ch)
		return
	}
	if s.combinedState(ch) == safety.StateSafe {
		// 任一故障源仍处于 SAFE（例如恢复帧数尚未集齐或看门狗未恢复）：
		// 报文计入恢复判定，但载荷不生效。
		s.reconcile(ch)
		return
	}
	ch.busSeq = seqVal // seqErr!=nil 时为 0
	switch m.Type {
	case protocol.TypeControl:
		if v, err := protocol.GetF64(m, protocol.TagValue); err == nil && r != busfault.AcceptedReordered {
			ch.busValue = v
			ch.output = v
		}
	case protocol.TypeSample:
		if v, err := protocol.GetF64(m, protocol.TagValue); err == nil && r != busfault.AcceptedReordered {
			ch.busValue = v
		}
	}
	s.reconcile(ch)
}

// FinishBus 结束复用总线输入，残留不完整帧时返回 ErrTruncated。
func (s *System) FinishBus() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dec.Finish()
}

// Snapshot 返回系统状态一致性快照。
func (s *System) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := Snapshot{Now: s.clock.Now(), Channels: map[string]ChannelStatus{}, BusUnrouted: s.unrouted}
	for _, id := range s.chanIDs {
		ch := s.chans[id]
		snap.Channels[id] = ChannelStatus{
			State:          s.combinedState(ch).String(),
			ConsecMisses:   ch.wd.Misses(),
			LateTotal:      ch.lateTotal,
			LastValue:      ch.lastValue,
			LastSeq:        ch.lastSeq,
			Output:         ch.output,
			BusState:       s.busStateString(ch),
			BusConsecFails: ch.bus.Consecutive(),
			BusAccepted:    ch.bus.AcceptedTotal(),
			BusRejected:    ch.bus.RejectedTotal(),
			BusLastSeq:     ch.busSeq,
			BusLastValue:   ch.busValue,
		}
	}
	return snap
}

// combinedState 汇总看门狗与总线监控（调用方持锁）。
func (s *System) combinedState(ch *channelState) safety.State {
	if ch.wd.State() == safety.StateSafe || ch.bus.Safe() {
		return safety.StateSafe
	}
	return safety.StateOK
}

func (s *System) busStateString(ch *channelState) string {
	if ch.bus.Safe() {
		return safety.StateSafe.String()
	}
	return safety.StateOK.String()
}
