// Package protocol 实现带版本的总线报文编解码。
//
// 帧格式（大端序）：
//
//	magic(2) | version(1) | kind(1) | hdrLen(2) | seq(4) | payloadLen(2) | payload | crc32(4)
//
// 版本兼容规则：
//   - hdrLen 表示完整头部长度，高版本可在 seq 之后追加扩展头字段，
//     旧解码器按 hdrLen 跳过未知字段，新增字段不影响既有处理。
//   - 版本号在 [MinSupportedVersion, MaxSupportedVersion] 内的报文被接受；
//     更高版本以 ErrUnsupportedVersion 明确拒绝，但解码器无状态，
//     单次失败不影响后续报文。
//   - 截断、CRC 校验失败、字段非法分别返回可区分的错误。
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// 协议常量。
const (
	Magic0 = 0x53 // 'S'
	Magic1 = 0x43 // 'C'

	// MinSupportedVersion 是可接受的最低协议版本。
	MinSupportedVersion = 1
	// MaxSupportedVersion 是可接受的最高协议版本。
	MaxSupportedVersion = 2

	// baseHeaderLen 是 V1 基础头部长度（含 magic 与 hdrLen 自身）。
	baseHeaderLen = 12
	crcLen        = 4
	// MaxPayload 是单帧负载上限。
	MaxPayload = 1024
)

// 可区分的解码失败原因，均可用 errors.Is 判断。
var (
	ErrTruncated          = errors.New("protocol: truncated frame")
	ErrChecksum           = errors.New("protocol: checksum mismatch")
	ErrUnsupportedVersion = errors.New("protocol: unsupported version")
	ErrInvalidMagic       = errors.New("protocol: invalid magic")
	ErrInvalidField       = errors.New("protocol: invalid field")
	ErrPayloadTooLarge    = errors.New("protocol: payload too large")
)

// Message 是一帧总线报文的逻辑内容。
type Message struct {
	Version uint8
	Kind    uint8
	Seq     uint32
	Payload []byte
}

// Encode 将报文编码为一帧字节流。
func Encode(m Message) ([]byte, error) {
	if m.Version < MinSupportedVersion || m.Version > MaxSupportedVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, m.Version)
	}
	if len(m.Payload) > MaxPayload {
		return nil, fmt.Errorf("%w: %d > %d", ErrPayloadTooLarge, len(m.Payload), MaxPayload)
	}
	hdrLen := baseHeaderLen
	buf := make([]byte, hdrLen+len(m.Payload)+crcLen)
	buf[0] = Magic0
	buf[1] = Magic1
	buf[2] = m.Version
	buf[3] = m.Kind
	binary.BigEndian.PutUint16(buf[4:6], uint16(hdrLen))
	binary.BigEndian.PutUint32(buf[6:10], m.Seq)
	binary.BigEndian.PutUint16(buf[10:12], uint16(len(m.Payload)))
	copy(buf[12:], m.Payload)
	crc := crc32.ChecksumIEEE(buf[:len(buf)-crcLen])
	binary.BigEndian.PutUint32(buf[len(buf)-crcLen:], crc)
	return buf, nil
}

// Decode 从 data 起始处解码一帧报文，返回报文与消耗的字节数。
// 解码器无状态：一次失败不会污染后续解码；调用方可按消耗长度推进流。
func Decode(data []byte) (Message, int, error) {
	if len(data) < baseHeaderLen {
		return Message{}, 0, ErrTruncated
	}
	if data[0] != Magic0 || data[1] != Magic1 {
		return Message{}, 0, ErrInvalidMagic
	}
	version := data[2]
	if version < MinSupportedVersion || version > MaxSupportedVersion {
		return Message{}, 0, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}
	hdrLen := int(binary.BigEndian.Uint16(data[4:6]))
	if hdrLen < baseHeaderLen {
		return Message{}, 0, fmt.Errorf("%w: hdrLen %d", ErrInvalidField, hdrLen)
	}
	if len(data) < hdrLen+2 {
		return Message{}, 0, ErrTruncated
	}
	// payloadLen 位于头部最后两个字节，扩展头字段按 hdrLen 跳过。
	payloadLen := int(binary.BigEndian.Uint16(data[hdrLen-2 : hdrLen]))
	if payloadLen > MaxPayload {
		return Message{}, 0, fmt.Errorf("%w: payloadLen %d", ErrInvalidField, payloadLen)
	}
	total := hdrLen + payloadLen + crcLen
	if len(data) < total {
		return Message{}, 0, ErrTruncated
	}
	frame := data[:total]
	want := binary.BigEndian.Uint32(frame[total-crcLen:])
	if crc32.ChecksumIEEE(frame[:total-crcLen]) != want {
		return Message{}, 0, ErrChecksum
	}
	payload := make([]byte, payloadLen)
	copy(payload, frame[hdrLen:hdrLen+payloadLen])
	return Message{
		Version: version,
		Kind:    data[3],
		Seq:     binary.BigEndian.Uint32(data[6:10]),
		Payload: payload,
	}, total, nil
}
