// Package control 组合调度、协议与安全降级，提供并发安全的控制器。
//
// 数据流：调度器派发采样 -> 设备产生总线报文 -> 协议解码校验 ->
// 看门狗记账 -> 应用采样值。所有共享状态由单一互斥锁保护，
// 并发的采样、控制与状态读取不会出现半更新或数据竞争。
package control

import (
	"errors"
	"fmt"
	"sync"

	"github.com/highcumontoa/realtime-sensor-control-go/clock"
	"github.com/highcumontoa/realtime-sensor-control-go/protocol"
	"github.com/highcumontoa/realtime-sensor-control-go/safety"
	"github.com/highcumontoa/realtime-sensor-control-go/schedule"
)

// ErrStaleSeq 表示报文序号回退/重复，属于时序异常。
var ErrStaleSeq = errors.New("control: stale sequence number")

// Device 是采样设备抽象，返回一帧已编码的总线报文。
type Device interface {
	Sample(id string) ([]byte, error)
}

// ChannelState 是一路通道的一致性快照。
type ChannelState struct {
	ID        string
	HasSample bool   // 是否持有有效采样
	Payload   []byte // 最近一次有效采样的负载
	LastSeq   uint32 // 最近一次有效采样的序号
	LastAt    int64  // 最近一次有效采样的时刻
	Late      bool   // 最近一次采样是否迟到
	Stale     bool   // 通道处于安全态，故障期数据不可沿用
	Misses    int    // 累计错过/异常次数
	Safety    safety.State
}

// Snapshot 是整个控制器的一致性快照。
type Snapshot struct {
	At       int64
	State    safety.State
	Channels []ChannelState
}

type channel struct {
	wd       *safety.Watchdog
	hasSampl bool
	payload  []byte
	lastSeq  uint32
	lastAt   int64
	late     bool
	misses   int
}

// Controller 是实时采样与安全控制的核心。
type Controller struct {
	mu       sync.Mutex
	clk      *clock.ManualClock
	sched    *schedule.Scheduler
	channels map[string]*channel
	order    []string // 注册顺序，保证快照确定
}

// New 创建控制器。
func New(clk *clock.ManualClock) *Controller {
	return &Controller{
		clk:      clk,
		sched:    schedule.New(clk),
		channels: make(map[string]*channel),
	}
}

// AddChannel 注册一路采样通道。
func (c *Controller) AddChannel(id string, period int64, priority int, policy schedule.MissPolicy, maxMisses int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sched.Add(schedule.Task{ID: id, Period: period, Priority: priority, Policy: policy})
	c.channels[id] = &channel{wd: safety.NewWatchdog(maxMisses)}
	c.order = append(c.order, id)
}

// Clock 返回控制器使用的时钟（用于推进时间）。
func (c *Controller) Clock() *clock.ManualClock { return c.clk }

// RunDue 处理当前时刻所有到期时隙：调度结果与每路看门狗记账一一对应。
func (c *Controller) RunDue(dev Device) []schedule.Outcome {
	c.mu.Lock()
	defer c.mu.Unlock()
	outcomes := c.sched.RunDue(func(id string) error {
		return c.sampleLocked(dev, id)
	})
	// 看门狗记账必须按结果的时间顺序统一进行：
	// Miss 与 Feed 交错顺序决定了最终状态，不得分开处理。
	for _, oc := range outcomes {
		ch := c.channels[oc.TaskID]
		switch {
		case oc.Missed:
			ch.wd.Miss()
			ch.misses++
		case oc.Err != nil:
			ch.wd.Miss()
			ch.misses++
		default:
			ch.wd.Feed()
			ch.late = oc.Late
		}
	}
	return outcomes
}

// sampleLocked 执行一次采样：取报文、解码、校验时序并应用采样值。
// 看门狗记账由 RunDue 按时间顺序统一完成，此处不触碰看门狗。
// 调用时必须已持有 c.mu。
func (c *Controller) sampleLocked(dev Device, id string) error {
	ch := c.channels[id]
	frame, err := dev.Sample(id)
	if err != nil {
		return fmt.Errorf("device: %w", err)
	}
	msg, _, err := protocol.Decode(frame)
	if err != nil {
		return err
	}
	if ch.hasSampl && msg.Seq <= ch.lastSeq {
		return ErrStaleSeq
	}
	// 全部校验通过才应用，避免半更新。
	ch.hasSampl = true
	ch.payload = append(ch.payload[:0], msg.Payload...)
	ch.lastSeq = msg.Seq
	ch.lastAt = c.clk.Now()
	return nil
}

// Snapshot 返回全部通道与全局安全状态的一致性快照。
func (c *Controller) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := Snapshot{At: c.clk.Now(), State: safety.StateNormal}
	for _, id := range c.order {
		ch := c.channels[id]
		st := ch.wd.State()
		if st == safety.StateSafe {
			snap.State = safety.StateSafe
		}
		cs := ChannelState{
			ID:        id,
			HasSample: ch.hasSampl,
			LastSeq:   ch.lastSeq,
			LastAt:    ch.lastAt,
			Late:      ch.late,
			Stale:     st == safety.StateSafe,
			Misses:    ch.misses,
			Safety:    st,
		}
		if ch.hasSampl {
			cs.Payload = append([]byte(nil), ch.payload...)
		}
		snap.Channels = append(snap.Channels, cs)
	}
	return snap
}
