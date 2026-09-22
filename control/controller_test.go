package control

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/realtime-sensor-control-go/clock"
	"github.com/highcumontoa/realtime-sensor-control-go/protocol"
	"github.com/highcumontoa/realtime-sensor-control-go/safety"
	"github.com/highcumontoa/realtime-sensor-control-go/schedule"
)

// simDevice 是可注入故障的模拟采样设备。
type simDevice struct {
	mu      sync.Mutex
	seq     map[string]uint32
	corrupt map[string]bool
	fail    map[string]bool
}

func newSimDevice() *simDevice {
	return &simDevice{
		seq:     make(map[string]uint32),
		corrupt: make(map[string]bool),
		fail:    make(map[string]bool),
	}
}

func (d *simDevice) Sample(id string) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail[id] {
		return nil, fmt.Errorf("simulated device fault on %s", id)
	}
	d.seq[id]++
	payload := []byte(fmt.Sprintf("%s-%04d", id, d.seq[id]))
	frame, err := protocol.Encode(protocol.Message{
		Version: 1, Kind: 1, Seq: d.seq[id], Payload: payload,
	})
	if err != nil {
		return nil, err
	}
	if d.corrupt[id] {
		frame[len(frame)-1] ^= 0xFF // 破坏 CRC
	}
	return frame, nil
}

func (d *simDevice) setCorrupt(id string, on bool) { d.mu.Lock(); d.corrupt[id] = on; d.mu.Unlock() }
func (d *simDevice) setFail(id string, on bool)    { d.mu.Lock(); d.fail[id] = on; d.mu.Unlock() }

func channelOf(s Snapshot, id string) ChannelState {
	for _, c := range s.Channels {
		if c.ID == id {
			return c
		}
	}
	panic("no channel " + id)
}

func TestNormalSampling(t *testing.T) {
	clk := clock.NewManual(0)
	c := New(clk)
	c.AddChannel("temp", 100, 1, schedule.MissMarkLate, 3)
	dev := newSimDevice()
	for i := 0; i < 5; i++ {
		clk.Advance(100)
		c.RunDue(dev)
	}
	snap := c.Snapshot()
	ch := channelOf(snap, "temp")
	if !ch.HasSample || ch.LastSeq != 5 || ch.Stale || ch.Safety != safety.StateNormal {
		t.Fatalf("unexpected channel state: %+v", ch)
	}
	if string(ch.Payload) != "temp-0005" {
		t.Fatalf("payload = %q", ch.Payload)
	}
	if snap.State != safety.StateNormal {
		t.Fatalf("global state = %v", snap.State)
	}
}

func TestMissedDeadlinesTripSafeState(t *testing.T) {
	clk := clock.NewManual(0)
	c := New(clk)
	c.AddChannel("pressure", 100, 1, schedule.MissSkip, 2)
	dev := newSimDevice()
	clk.Advance(250) // 错过 t=100 与 t=200 两个时隙
	outcomes := c.RunDue(dev)
	var missed int
	for _, o := range outcomes {
		if o.Missed {
			missed++
		}
	}
	if missed != 2 {
		t.Fatalf("missed = %d, want 2", missed)
	}
	snap := c.Snapshot()
	if snap.State != safety.StateSafe {
		t.Fatalf("global state = %v, want SAFE", snap.State)
	}
	ch := channelOf(snap, "pressure")
	if ch.Safety != safety.StateSafe || !ch.Stale {
		t.Fatalf("channel should be SAFE and stale: %+v", ch)
	}
}

func TestCorruptedFramesTripAndRecover(t *testing.T) {
	clk := clock.NewManual(0)
	c := New(clk)
	c.AddChannel("flow", 100, 1, schedule.MissMarkLate, 2)
	dev := newSimDevice()

	// 正常采样一次。
	clk.Advance(100)
	c.RunDue(dev)
	if channelOf(c.Snapshot(), "flow").LastSeq != 1 {
		t.Fatal("expected first sample")
	}

	// 注入报文损坏：连续两帧 CRC 失败，进入安全态。
	dev.setCorrupt("flow", true)
	clk.Advance(100)
	c.RunDue(dev)
	clk.Advance(100)
	c.RunDue(dev)
	snap := c.Snapshot()
	ch := channelOf(snap, "flow")
	if ch.Safety != safety.StateSafe || !ch.Stale {
		t.Fatalf("expected SAFE+stale after corrupted frames: %+v", ch)
	}
	if ch.LastSeq != 1 {
		t.Fatalf("corrupted frames must not be applied, LastSeq = %d", ch.LastSeq)
	}

	// 恢复正常报文：立即回到正常态，故障期状态不被沿用。
	dev.setCorrupt("flow", false)
	clk.Advance(100)
	c.RunDue(dev)
	snap = c.Snapshot()
	ch = channelOf(snap, "flow")
	if ch.Safety != safety.StateNormal || ch.Stale {
		t.Fatalf("expected recovery to NORMAL: %+v", ch)
	}
	if ch.LastSeq != 4 || string(ch.Payload) != "flow-0004" {
		t.Fatalf("recovery sample not applied: %+v", ch)
	}
	// 恢复后单次异常不应立即触发（故障期计数已清零）。
	dev.setCorrupt("flow", true)
	clk.Advance(100)
	c.RunDue(dev)
	if channelOf(c.Snapshot(), "flow").Safety != safety.StateNormal {
		t.Fatal("tripped immediately after recovery: fault state reused")
	}
}

func TestDeviceFailureCountsAsMiss(t *testing.T) {
	clk := clock.NewManual(0)
	c := New(clk)
	c.AddChannel("volt", 100, 1, schedule.MissMarkLate, 2)
	dev := newSimDevice()
	dev.setFail("volt", true)
	clk.Advance(100)
	c.RunDue(dev)
	clk.Advance(100)
	c.RunDue(dev)
	if c.Snapshot().State != safety.StateSafe {
		t.Fatal("device failures should trip safe state")
	}
}

func TestConcurrentAccessNoRace(t *testing.T) {
	clk := clock.NewManual(0)
	c := New(clk)
	c.AddChannel("a", 10, 1, schedule.MissMarkLate, 3)
	c.AddChannel("b", 20, 2, schedule.MissSkip, 3)
	dev := newSimDevice()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 状态读取方。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					snap := c.Snapshot()
					if len(snap.Channels) != 2 {
						t.Error("partial snapshot observed")
						return
					}
				}
			}
		}()
	}
	// 采样推进方（单写者推进时钟，模拟主循环）。
	for i := 0; i < 200; i++ {
		clk.Advance(10)
		c.RunDue(dev)
	}
	close(stop)
	wg.Wait()
}

func TestSnapshotIsolation(t *testing.T) {
	clk := clock.NewManual(0)
	c := New(clk)
	c.AddChannel("x", 100, 1, schedule.MissMarkLate, 3)
	dev := newSimDevice()
	clk.Advance(100)
	c.RunDue(dev)
	snap := c.Snapshot()
	// 修改快照不影响控制器内部状态。
	snap.Channels[0].Payload[0] = 'Z'
	if bytes.HasPrefix(channelOf(c.Snapshot(), "x").Payload, []byte("Z")) {
		t.Fatal("snapshot aliases internal state")
	}
}

func TestOutcomeErrorIsDistinguishable(t *testing.T) {
	clk := clock.NewManual(0)
	c := New(clk)
	c.AddChannel("y", 100, 1, schedule.MissMarkLate, 5)
	dev := newSimDevice()
	dev.setCorrupt("y", true)
	clk.Advance(100)
	outcomes := c.RunDue(dev)
	if len(outcomes) != 1 || !errors.Is(outcomes[0].Err, protocol.ErrChecksum) {
		t.Fatalf("expected checksum error outcome, got %+v", outcomes)
	}
}
