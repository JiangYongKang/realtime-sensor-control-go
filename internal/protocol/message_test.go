package protocol

import (
	"errors"
	"testing"
)

func sampleMsg() Message {
	return Message{
		Version: CurrentVersion,
		Type:    TypeSample,
		Fields: map[uint8][]byte{
			TagSeq:   U64(42),
			TagValue: F64(21.5),
		},
	}
}

func decodeOne(t *testing.T, data []byte) Message {
	t.Helper()
	d := NewDecoder(TagSeq, TagValue, TagChannel)
	msgs, errs := d.Feed(data)
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d msgs, want 1", len(msgs))
	}
	return msgs[0]
}

func TestRoundTrip(t *testing.T) {
	m := decodeOne(t, Encode(sampleMsg()))
	seq, err := GetU64(m, TagSeq)
	if err != nil || seq != 42 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}
	v, err := GetF64(m, TagValue)
	if err != nil || v != 21.5 {
		t.Fatalf("value=%v err=%v", v, err)
	}
}

func TestBadCRCRejectedAndResync(t *testing.T) {
	good := Encode(sampleMsg())
	bad := append([]byte(nil), good...)
	bad[len(bad)-1] ^= 0xFF
	d := NewDecoder(TagSeq, TagValue)
	msgs, errs := d.Feed(append(bad, good...))
	if len(msgs) != 1 {
		t.Fatalf("got %d msgs, want 1 (后续正常报文必须不受影响)", len(msgs))
	}
	if len(errs) != 1 || !errors.Is(errs[0], ErrBadCRC) {
		t.Fatalf("errs=%v, want [ErrBadCRC]", errs)
	}
}

func TestTruncatedReportedOnFinish(t *testing.T) {
	good := Encode(sampleMsg())
	d := NewDecoder(TagSeq, TagValue)
	msgs, errs := d.Feed(good[:len(good)-3])
	if len(msgs) != 0 || len(errs) != 0 {
		t.Fatalf("流式等待中不应产生结果: msgs=%d errs=%v", len(msgs), errs)
	}
	if err := d.Finish(); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Finish err=%v, want ErrTruncated", err)
	}
}

func TestBadMagicResync(t *testing.T) {
	good := Encode(sampleMsg())
	d := NewDecoder(TagSeq, TagValue)
	msgs, errs := d.Feed(append([]byte{0x00, 0x01, 0x02}, good...))
	if len(msgs) != 1 || len(errs) != 1 || !errors.Is(errs[0], ErrBadMagic) {
		t.Fatalf("msgs=%d errs=%v", len(msgs), errs)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	m := sampleMsg()
	m.Version = 0
	d := NewDecoder(TagSeq, TagValue)
	msgs, errs := d.Feed(Encode(m))
	if len(msgs) != 0 || len(errs) != 1 || !errors.Is(errs[0], ErrUnsupportedVersion) {
		t.Fatalf("msgs=%d errs=%v", len(msgs), errs)
	}
}

func TestHigherVersionAndUnknownFieldsTolerated(t *testing.T) {
	m := sampleMsg()
	m.Version = CurrentVersion + 5 // 高版本
	m.Unknown = map[uint8][]byte{200: {1, 2, 3}}
	got := decodeOne(t, Encode(m))
	if got.Version != CurrentVersion+5 {
		t.Fatalf("version=%d", got.Version)
	}
	seq, err := GetU64(got, TagSeq)
	if err != nil || seq != 42 {
		t.Fatalf("既有字段处理失效: seq=%d err=%v", seq, err)
	}
	if string(got.Unknown[200]) != string([]byte{1, 2, 3}) {
		t.Fatalf("未知字段未保留: %v", got.Unknown)
	}
}

func TestInvalidFieldRejected(t *testing.T) {
	m := sampleMsg()
	m.Fields[TagSeq] = []byte{1, 2, 3} // 长度非法（应为 8）
	d := NewDecoder(TagSeq, TagValue)
	msgs, errs := d.Feed(Encode(m))
	if len(msgs) != 0 || len(errs) != 1 || !errors.Is(errs[0], ErrInvalidField) {
		t.Fatalf("msgs=%d errs=%v", len(msgs), errs)
	}
}

func TestUnknownTagsIgnoredByReceiverWithoutThem(t *testing.T) {
	// 接收端不认识 TagValue：进入 Unknown，不影响已知字段。
	m := sampleMsg()
	d := NewDecoder(TagSeq)
	msgs, errs := d.Feed(Encode(m))
	if len(errs) != 0 || len(msgs) != 1 {
		t.Fatalf("msgs=%d errs=%v", len(msgs), errs)
	}
	if _, ok := msgs[0].Unknown[TagValue]; !ok {
		t.Fatalf("未知 tag 应进入 Unknown: %+v", msgs[0])
	}
}

func TestBoundedDecoderDropsOverrunAndRecovers(t *testing.T) {
	d := NewBoundedDecoder(128, TagSeq, TagValue)
	// 先放入一帧不完整数据作为残留。
	partial := Encode(sampleMsg())[:20]
	if _, errs := d.Feed(partial); len(errs) != 0 {
		t.Fatalf("等待中的残帧不应报错: %v", errs)
	}
	// 再喂入超过上限的数据：残留被丢弃并报告 ErrTruncated，随后正常帧照解。
	good := Encode(sampleMsg())
	huge := make([]byte, 200)
	msgs, errs := d.Feed(append(huge, good...))
	foundTrunc := false
	for _, e := range errs {
		if e == ErrTruncated {
			foundTrunc = true
		}
	}
	if !foundTrunc {
		t.Fatalf("超限积压应报告 ErrTruncated, errs=%v", errs)
	}
	if len(msgs) != 1 {
		t.Fatalf("积压丢弃后正常帧应继续解码, got %d", len(msgs))
	}
}
