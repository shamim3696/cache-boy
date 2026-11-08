package snapshot

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"math"

	"cache-boy/internal/datatype/bloom"
	"cache-boy/internal/datatype/hll"
)

// Low-level primitives of the snapshot format. Every integer is a varint
// (encoding/binary's LEB128-style uvarint; signed values use zigzag varints),
// every string is a uvarint length followed by the raw bytes, and floats are
// 8 (float64) or 4 (float32) little-endian IEEE-754 bytes so they round-trip
// exactly, including -0, ±Inf and NaN payloads.

// maxString caps a single string/blob length while decoding, so a corrupted
// length prefix fails cleanly instead of trying to allocate gigabytes.
const maxString = 512 << 20 // same as resp.MaxBulkLen

// maxCount caps element counts (list items, hash fields...) for the same reason.
const maxCount = 1 << 31

// encoder appends to an in-memory buffer. Encoding never fails; the snapshot
// is written to its destination in one go afterwards.
type encoder struct {
	buf []byte
}

func (w *encoder) byte(b byte)      { w.buf = append(w.buf, b) }
func (w *encoder) uvarint(n uint64) { w.buf = binary.AppendUvarint(w.buf, n) }
func (w *encoder) varint(n int64)   { w.buf = binary.AppendVarint(w.buf, n) }
func (w *encoder) string(s string) {
	w.uvarint(uint64(len(s)))
	w.buf = append(w.buf, s...)
}
func (w *encoder) bytes(b []byte) {
	w.uvarint(uint64(len(b)))
	w.buf = append(w.buf, b...)
}
func (w *encoder) float64(f float64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, math.Float64bits(f))
}
func (w *encoder) float32(f float32) {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, math.Float32bits(f))
}

// decoder reads from a byte reader, feeding every byte it consumes into the
// running CRC so the trailing checksum can be verified. It tracks the offset
// for error messages.
type decoder struct {
	r   *bufio.Reader
	crc hash.Hash64
	off int64
	one [1]byte
}

// CorruptError reports a malformed snapshot.
type CorruptError struct {
	Offset int64
	Msg    string
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("snapshot corrupt at byte %d: %s", e.Offset, e.Msg)
}

func (d *decoder) fail(format string, args ...any) error {
	return &CorruptError{Offset: d.off, Msg: fmt.Sprintf(format, args...)}
}

// eof converts an unexpected end of input into a CorruptError.
func (d *decoder) eof(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return d.fail("unexpected end of data (file truncated?)")
	}
	return err
}

func (d *decoder) ReadByte() (byte, error) {
	b, err := d.r.ReadByte()
	if err != nil {
		return 0, d.eof(err)
	}
	d.one[0] = b
	d.crc.Write(d.one[:])
	d.off++
	return b, nil
}

func (d *decoder) full(p []byte) error {
	if _, err := io.ReadFull(d.r, p); err != nil {
		return d.eof(err)
	}
	d.crc.Write(p)
	d.off += int64(len(p))
	return nil
}

func (d *decoder) uvarint() (uint64, error) {
	n, err := binary.ReadUvarint(d)
	if err != nil {
		if _, ok := err.(*CorruptError); ok {
			return 0, err
		}
		return 0, d.fail("bad varint: %v", err)
	}
	return n, nil
}

func (d *decoder) varint() (int64, error) {
	n, err := binary.ReadVarint(d)
	if err != nil {
		if _, ok := err.(*CorruptError); ok {
			return 0, err
		}
		return 0, d.fail("bad varint: %v", err)
	}
	return n, nil
}

func (d *decoder) count() (int, error) {
	n, err := d.uvarint()
	if err != nil {
		return 0, err
	}
	if n > maxCount {
		return 0, d.fail("element count %d is too large", n)
	}
	return int(n), nil
}

func (d *decoder) bytes() ([]byte, error) {
	n, err := d.uvarint()
	if err != nil {
		return nil, err
	}
	if n > maxString {
		return nil, d.fail("string length %d is too large", n)
	}
	b := make([]byte, n)
	return b, d.full(b)
}

func (d *decoder) string() (string, error) {
	b, err := d.bytes()
	return string(b), err
}

func (d *decoder) float64() (float64, error) {
	var b [8]byte
	if err := d.full(b[:]); err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b[:])), nil
}

func (d *decoder) float32() (float32, error) {
	var b [4]byte
	if err := d.full(b[:]); err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(b[:])), nil
}

// Bloom filters and HyperLogLogs are stored as one string holding the
// type's own encoding, which validates itself on decode.

func (d *decoder) bloom() (*bloom.Filter, error) {
	b, err := d.bytes()
	if err != nil {
		return nil, err
	}
	f, err := bloom.Unmarshal(b)
	if err != nil {
		return nil, d.fail("invalid bloom filter: %v", err)
	}
	return f, nil
}

func (d *decoder) hll() (*hll.HLL, error) {
	b, err := d.bytes()
	if err != nil {
		return nil, err
	}
	h, err := hll.Unmarshal(b)
	if err != nil {
		return nil, d.fail("invalid hyperloglog: %v", err)
	}
	return h, nil
}
