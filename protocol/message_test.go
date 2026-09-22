package protocol

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	m := Message{Version: 1, Kind: 7, Seq: 42, Payload: []byte("hello")}
	frame, err := Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	got, n, err := Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(frame) {
		t.Fatalf("consumed %d, want %d", n, len(frame))
	}
	if got.Version != 1 || got.Kind != 7 || got.Seq != 42 || string(got.Payload) != "hello" {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}

func TestTruncated(t *testing.T) {
	frame, _ := Encode(Message{Version: 1, Kind: 1, Seq: 1, Payload: []byte("abcdef")})
	for _, cut := range []int{0, 3, baseHeaderLen - 1, baseHeaderLen, len(frame) - 1} {
		if _, _, err := Decode(frame[:cut]); !errors.Is(err, ErrTruncated) {
			t.Fatalf("cut=%d: err = %v, want ErrTruncated", cut, err)
		}
	}
}

func TestChecksumFailure(t *testing.T) {
	frame, _ := Encode(Message{Version: 1, Kind: 1, Seq: 1, Payload: []byte("data")})
	frame[len(frame)-1] ^= 0xFF
	if _, _, err := Decode(frame); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
	// 负载字节被篡改同样被 CRC 发现。
	frame2, _ := Encode(Message{Version: 1, Kind: 1, Seq: 1, Payload: []byte("data")})
	frame2[baseHeaderLen] ^= 0x01
	if _, _, err := Decode(frame2); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestInvalidMagic(t *testing.T) {
	frame, _ := Encode(Message{Version: 1, Kind: 1, Seq: 1})
	frame[0] = 0x00
	if _, _, err := Decode(frame); !errors.Is(err, ErrInvalidMagic) {
		t.Fatalf("err = %v, want ErrInvalidMagic", err)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	frame, _ := Encode(Message{Version: 1, Kind: 1, Seq: 1})
	frame[2] = MaxSupportedVersion + 1
	// 重新计算 CRC，确保拒绝原因是版本而非校验和。
	crc := crc32.ChecksumIEEE(frame[:len(frame)-crcLen])
	binary.BigEndian.PutUint32(frame[len(frame)-crcLen:], crc)
	if _, _, err := Decode(frame); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("err = %v, want ErrUnsupportedVersion", err)
	}
	if _, err := Encode(Message{Version: MaxSupportedVersion + 1}); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("encode err = %v, want ErrUnsupportedVersion", err)
	}
}

// TestExtendedHeaderForwardCompat 构造 V2 报文：在基础头之后追加扩展字段，
// 旧字段布局不变，解码器应按 hdrLen 跳过未知字段。
func TestExtendedHeaderForwardCompat(t *testing.T) {
	payload := []byte("v2payload")
	hdrLen := baseHeaderLen + 4 // 追加 4 字节扩展字段
	frame := make([]byte, hdrLen+len(payload)+crcLen)
	frame[0], frame[1] = Magic0, Magic1
	frame[2] = 2 // version
	frame[3] = 9 // kind
	binary.BigEndian.PutUint16(frame[4:6], uint16(hdrLen))
	binary.BigEndian.PutUint32(frame[6:10], 777) // seq
	// 扩展字段（未知，应被跳过）。
	binary.BigEndian.PutUint32(frame[10:14], 0xDEADBEEF)
	// payloadLen 位于头部最后两字节。
	binary.BigEndian.PutUint16(frame[hdrLen-2:hdrLen], uint16(len(payload)))
	copy(frame[hdrLen:], payload)
	crc := crc32.ChecksumIEEE(frame[:len(frame)-crcLen])
	binary.BigEndian.PutUint32(frame[len(frame)-crcLen:], crc)

	msg, n, err := Decode(frame)
	if err != nil {
		t.Fatalf("v2 extended header rejected: %v", err)
	}
	if n != len(frame) || msg.Version != 2 || msg.Kind != 9 || msg.Seq != 777 || string(msg.Payload) != "v2payload" {
		t.Fatalf("v2 decode mismatch: %+v n=%d", msg, n)
	}
}

// TestFailureDoesNotPoisonStream 一次失败不影响后续正常报文。
func TestFailureDoesNotPoisonStream(t *testing.T) {
	good1, _ := Encode(Message{Version: 1, Kind: 1, Seq: 1, Payload: []byte("a")})
	bad, _ := Encode(Message{Version: 1, Kind: 1, Seq: 2, Payload: []byte("b")})
	bad[len(bad)-1] ^= 0xFF
	good2, _ := Encode(Message{Version: 1, Kind: 1, Seq: 3, Payload: []byte("c")})

	stream := append(append(append([]byte{}, good1...), bad...), good2...)
	m1, n1, err := Decode(stream)
	if err != nil || m1.Seq != 1 {
		t.Fatalf("first frame: %v %+v", err, m1)
	}
	if _, _, err := Decode(stream[n1:]); !errors.Is(err, ErrChecksum) {
		t.Fatalf("second frame: err = %v, want ErrChecksum", err)
	}
	// 跳过损坏帧后，后续帧正常解码。
	m3, _, err := Decode(stream[n1+len(bad):])
	if err != nil || m3.Seq != 3 {
		t.Fatalf("third frame after failure: %v %+v", err, m3)
	}
}

func TestPayloadTooLarge(t *testing.T) {
	if _, err := Encode(Message{Version: 1, Payload: make([]byte, MaxPayload+1)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("err = %v, want ErrPayloadTooLarge", err)
	}
}
