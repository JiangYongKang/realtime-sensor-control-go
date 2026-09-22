// Package protocol 实现带版本的总线报文编解码。
//
// 帧格式：
//
//	[magic 2B: 0x53 0x43][version 1B][type 1B][payloadLen 2B BE][payload][crc32 4B IEEE]
//
// payload 为 TLV 字段序列：[tag 1B][len 1B][value]。
// 兼容规则：版本号 >= MinVersion 即接受；本端不认识的 tag 进入 Unknown
// 并被忽略，因此高版本新增字段不影响既有处理。
package protocol

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"sort"
)

// 可区分的解码失败原因。
var (
	ErrTruncated          = errors.New("protocol: truncated frame")
	ErrBadMagic           = errors.New("protocol: bad magic")
	ErrBadCRC             = errors.New("protocol: crc mismatch")
	ErrUnsupportedVersion = errors.New("protocol: unsupported version")
	ErrInvalidField       = errors.New("protocol: invalid field")
)

const (
	magic0   = 0x53
	magic1   = 0x43
	headLen  = 6
	crcLen   = 4
	maxPay   = 1 << 16
	// MinVersion 是可接受的最低协议版本。
	MinVersion = 1
	// CurrentVersion 是本端编码时使用的版本。
	CurrentVersion = 2
)

// 已知字段 tag。
const (
	TagSeq     uint8 = 1 // 8 字节大端序号
	TagValue   uint8 = 2 // 8 字节 IEEE754 float64
	TagChannel uint8 = 3 // 1..32 字节通道名
)

// fieldSpec 约束已知字段的长度范围，用于合法性校验。
type fieldSpec struct{ min, max int }

var fieldSpecs = map[uint8]fieldSpec{
	TagSeq:     {8, 8},
	TagValue:   {8, 8},
	TagChannel: {1, 32},
}

// Type 是报文类型。
type Type uint8

const (
	TypeSample  Type = 1
	TypeControl Type = 2
)

// Message 是解码后的报文。未知字段被保留在 Unknown 中以便转发。
type Message struct {
	Version uint8
	Type    Type
	Fields  map[uint8][]byte // 本端认识的字段（TLV tag -> value）
	Unknown map[uint8][]byte // 高版本新增字段，不参与处理但保留
}

// Encode 将报文编码为带帧头与 CRC 的字节流，字段按 tag 排序保证确定性。
func Encode(m Message) []byte {
	var payload []byte
	tags := make([]int, 0, len(m.Fields)+len(m.Unknown))
	for t := range m.Fields {
		tags = append(tags, int(t))
	}
	for t := range m.Unknown {
		tags = append(tags, int(t))
	}
	sort.Ints(tags)
	for _, t := range tags {
		tag := uint8(t)
		v, ok := m.Fields[tag]
		if !ok {
			v = m.Unknown[tag]
		}
		payload = append(payload, tag, byte(len(v)))
		payload = append(payload, v...)
	}
	frame := make([]byte, 0, headLen+len(payload)+crcLen)
	frame = append(frame, magic0, magic1, m.Version, byte(m.Type),
		byte(len(payload)>>8), byte(len(payload)))
	frame = append(frame, payload...)
	crc := crc32.ChecksumIEEE(frame)
	var c [4]byte
	binary.BigEndian.PutUint32(c[:], crc)
	return append(frame, c[:]...)
}

// Decoder 从字节流中逐帧解码，单帧失败不影响后续帧。
type Decoder struct {
	known map[uint8]bool
	buf   []byte
}

// NewDecoder 创建解码器，knownTags 为本端认识的字段 tag 集合。
func NewDecoder(knownTags ...uint8) *Decoder {
	d := &Decoder{known: map[uint8]bool{}}
	for _, t := range knownTags {
		d.known[t] = true
	}
	return d
}

// Feed 追加数据并尽可能解码，返回成功解码的报文与逐帧错误（可区分原因）。
// 一次失败仅丢弃对应帧并重新同步到下一个 magic，后续正常报文不受影响。
func (d *Decoder) Feed(data []byte) (msgs []Message, errs []error) {
	d.buf = append(d.buf, data...)
	for {
		i := indexMagic(d.buf)
		if i < 0 {
			// 保留可能是 magic 前缀的最后一个字节。
			if n := len(d.buf); n > 0 && d.buf[n-1] == magic0 {
				d.buf = d.buf[n-1:]
			} else {
				d.buf = d.buf[:0]
			}
			return msgs, errs
		}
		if i > 0 {
			errs = append(errs, ErrBadMagic)
			d.buf = d.buf[i:]
		}
		if len(d.buf) < headLen {
			return msgs, errs // 等待更多数据
		}
		ver := d.buf[2]
		plen := int(d.buf[4])<<8 | int(d.buf[5])
		if ver < MinVersion {
			errs = append(errs, ErrUnsupportedVersion)
			d.buf = d.buf[1:] // 跳过一个字节重新同步
			continue
		}
		total := headLen + plen + crcLen
		if len(d.buf) < total {
			return msgs, errs // 帧不完整，等待更多数据
		}
		frame := d.buf[:total]
		d.buf = d.buf[total:]
		want := binary.BigEndian.Uint32(frame[total-crcLen:])
		if crc32.ChecksumIEEE(frame[:total-crcLen]) != want {
			errs = append(errs, ErrBadCRC)
			continue
		}
		msg, err := d.parsePayload(ver, Type(frame[3]), frame[headLen:total-crcLen])
		if err != nil {
			errs = append(errs, err)
			continue
		}
		msgs = append(msgs, msg)
	}
}

// Finish 在输入结束时调用；若缓冲区残留不完整帧则报告 ErrTruncated。
func (d *Decoder) Finish() error {
	if len(d.buf) > 0 {
		d.buf = d.buf[:0]
		return ErrTruncated
	}
	return nil
}

func (d *Decoder) parsePayload(ver uint8, typ Type, payload []byte) (Message, error) {
	m := Message{Version: ver, Type: typ, Fields: map[uint8][]byte{}, Unknown: map[uint8][]byte{}}
	for len(payload) > 0 {
		if len(payload) < 2 {
			return m, ErrInvalidField
		}
		tag, l := payload[0], int(payload[1])
		payload = payload[2:]
		if len(payload) < l {
			return m, ErrInvalidField
		}
		v := payload[:l]
		payload = payload[l:]
		if d.known[tag] {
			spec, ok := fieldSpecs[tag]
			if ok && (l < spec.min || l > spec.max) {
				return m, ErrInvalidField
			}
			m.Fields[tag] = append([]byte(nil), v...)
		} else {
			m.Unknown[tag] = append([]byte(nil), v...)
		}
	}
	return m, nil
}

func indexMagic(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if b[i] == magic0 && b[i+1] == magic1 {
			return i
		}
	}
	return -1
}

// U64 构造 8 字节大端字段值。
func U64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

// F64 构造 8 字节 float64 字段值。
func F64(v float64) []byte {
	return U64(math.Float64bits(v))
}

// GetU64 读取 8 字节大端字段，长度非法时返回 ErrInvalidField。
func GetU64(m Message, tag uint8) (uint64, error) {
	v, ok := m.Fields[tag]
	if !ok || len(v) != 8 {
		return 0, ErrInvalidField
	}
	return binary.BigEndian.Uint64(v), nil
}

// GetF64 读取 8 字节 float64 字段。
func GetF64(m Message, tag uint8) (float64, error) {
	u, err := GetU64(m, tag)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(u), nil
}
