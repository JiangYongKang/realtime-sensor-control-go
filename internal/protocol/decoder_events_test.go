package protocol

import (
	"errors"
	"testing"
)

func channelMsg(ch string) Message {
	return Message{
		Version: CurrentVersion,
		Type:    TypeSample,
		Fields: map[uint8][]byte{
			TagSeq:     U64(1),
			TagValue:   F64(21.5),
			TagChannel: []byte(ch),
		},
	}
}

func TestFeedEventsPreservesOrder(t *testing.T) {
	good := Encode(channelMsg("chA"))
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xFF
	d := NewDecoder(TagSeq, TagValue, TagChannel)
	events := d.FeedEvents(append(bad, good...))
	if len(events) != 2 {
		t.Fatalf("events=%d, want 2", len(events))
	}
	if events[0].Err == nil || events[1].Msg.Type == 0 {
		t.Fatalf("事件顺序错误: %+v %+v", events[0], events[1])
	}
	if !errors.Is(events[0].Err, ErrBadCRC) {
		t.Fatalf("events[0]=%v, want ErrBadCRC", events[0].Err)
	}
}

func TestFrameErrorCarriesChannel(t *testing.T) {
	good := Encode(channelMsg("chA"))
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xFF
	d := NewDecoder(TagSeq, TagValue, TagChannel)
	events := d.FeedEvents(bad)
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}
	fe, ok := events[0].Err.(*FrameError)
	if !ok {
		t.Fatalf("err 类型=%T, want *FrameError", events[0].Err)
	}
	if fe.Channel != "chA" {
		t.Fatalf("坏帧归属通道=%q, want chA", fe.Channel)
	}
}

func TestExtractChannel(t *testing.T) {
	good := Encode(channelMsg("chA"))
	if got := ExtractChannel(good[headLen : len(good)-crcLen]); got != "chA" {
		t.Fatalf("got %q, want chA", got)
	}
	// 无通道字段。
	m := Message{Version: CurrentVersion, Type: TypeSample,
		Fields: map[uint8][]byte{TagSeq: U64(1)}}
	b := Encode(m)
	if got := ExtractChannel(b[headLen : len(b)-crcLen]); got != "" {
		t.Fatalf("无通道字段应返回空, got %q", got)
	}
	// TLV 损坏（截断）。
	if got := ExtractChannel([]byte{TagChannel, 5, 1, 2}); got != "" {
		t.Fatalf("TLV 截断应返回空, got %q", got)
	}
}

func TestDecoderBufferBounded(t *testing.T) {
	d := NewDecoder(TagSeq, TagValue, TagChannel)
	// 大段垃圾：重新同步后缓冲立即清空。
	d.Feed(make([]byte, 1<<20))
	if d.Buffered() != 0 {
		t.Fatalf("垃圾后缓冲应清空, got %d", d.Buffered())
	}
	// 声明超大 payload 的帧头：等待中的缓冲被 maxPay 封顶。
	d.Feed([]byte{magic0, magic1, CurrentVersion, byte(TypeSample), 0xFF, 0xFF})
	if d.Buffered() > MaxBufferedFrame {
		t.Fatalf("缓冲 %d 超过上限 %d", d.Buffered(), MaxBufferedFrame)
	}
	// 继续喂部分数据，仍不得超过单帧上限。
	d.Feed(make([]byte, 1000))
	if d.Buffered() > MaxBufferedFrame {
		t.Fatalf("缓冲 %d 超过上限 %d", d.Buffered(), MaxBufferedFrame)
	}
}
