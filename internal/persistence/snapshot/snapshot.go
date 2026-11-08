// Package snapshot implements cache-boy's binary point-in-time dump (the
// equivalent of Redis' RDB file). The same bytes are used for the dump file
// on disk, the preamble of a rewritten AOF and the full-sync payload sent to
// replicas.
//
// # File layout
//
//		+----------+---------+----------+----------+-----+-----+--------------+
//		| CACHEBOY | version | record 1 | record 2 | ... | EOF | CRC-64 (8 B) |
//		+----------+---------+----------+----------+-----+-----+--------------+
//
//	  - magic: the 8 ASCII bytes "CACHEBOY".
//	  - version: one byte, currently 1. Readers refuse versions they don't know.
//	  - records: each starts with a one-byte opcode.
//	  - EOF: opcode 0xFF, followed by the CRC-64 (ECMA polynomial, little
//	    endian) of every byte before it, from the magic to the EOF opcode
//	    included. Nothing is applied before the checksum has been verified, so a
//	    damaged file can never half-load.
//
// # Records
//
//	KEY    (0x01): expireAt uvarint (unix ms, 0 = none) | kind byte | key string | payload
//	MODULE (0x02): argc uvarint | argc × string   — a command to replay
//
// Strings are a uvarint length + raw bytes. The payload depends on the kind:
//
//	string     value string (stored uncompressed; recompressed on load per config)
//	list       count | count × item
//	hash       count | count × (field, value)
//	set        count | count × member
//	zset       count | count × (member, score float64)            in rank order
//	json       document text
//	timeseries retention varint | nlabels | nlabels × (name, value) |
//	           nsamples | nsamples × (ts varint, value float64)
//	vector     count | count × (id, dim uvarint, dim × float32, attributes string)
//	bloom      bloom.Filter.Marshal() as a string
//	hll        hll.HLL.Marshal() as a string
//
// MODULE records carry state that feature modules keep outside the keyspace
// (queues, tags, rate limits...). Engine.DumpModules emits them as commands
// and Load replays them through the normal dispatcher once every key is in.
//
// # Consistency and speed
//
// Encode runs while the caller holds DB.LockAll(), so the dump is an exact
// point-in-time copy. It only serialises into memory (no I/O, no syscalls),
// which keeps the stop-the-world pause short; the caller releases the lock
// and then writes the bytes to disk or a socket. Redis forks to get the same
// effect; Go can't fork, so we pay for a temporary in-memory copy instead.
package snapshot

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/datatype/deque"
	"github.com/shamim3696/cache-boy/internal/datatype/jsondoc"
	"github.com/shamim3696/cache-boy/internal/datatype/skiplist"
	"github.com/shamim3696/cache-boy/internal/datatype/timeseries"
	"github.com/shamim3696/cache-boy/internal/datatype/vector"
	"github.com/shamim3696/cache-boy/internal/store"
)

const (
	// Magic starts every snapshot (and every AOF that has a snapshot preamble).
	Magic = "CACHEBOY"
	// Version is the format version written by this code.
	Version byte = 1

	opKey    byte = 0x01
	opModule byte = 0x02
	opEOF    byte = 0xFF
)

var crcTable = crc64.MakeTable(crc64.ECMA)

// ErrBadMagic means the input is not a cache-boy snapshot at all.
var ErrBadMagic = errors.New("snapshot: not a cache-boy snapshot (bad magic)")

// ErrChecksum means the data was read completely but does not match its CRC.
var ErrChecksum = errors.New("snapshot: checksum mismatch (file is corrupted)")

// Encode writes a snapshot of e's keyspace and module state to w.
// The caller must hold e.DB.LockAll(). Pass a *bytes.Buffer to keep the
// locked section free of I/O, then write the buffer out after unlocking.
func Encode(w io.Writer, e *command.Engine) error {
	return EncodeDB(w, e.DB, e.DumpModules)
}

// EncodeDB is Encode for a bare DB. dump, if not nil, emits module commands
// (normally Engine.DumpModules). The caller must hold db.LockAll().
func EncodeDB(w io.Writer, db *store.DB, dump func(emit func(args []string))) error {
	b, err := AppendDB(nil, db, dump)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// AppendDB appends a snapshot of db to buf and returns the extended slice.
// It avoids the extra copy of writing into an io.Writer. Requires LockAll.
func AppendDB(buf []byte, db *store.DB, dump func(emit func(args []string))) ([]byte, error) {
	if buf == nil {
		buf = make([]byte, 0, 64+db.UsedMemory())
	}
	start := len(buf)
	enc := &encoder{buf: buf}
	enc.buf = append(enc.buf, Magic...)
	enc.byte(Version)

	var err error
	db.ForEach(func(key string, e *store.Entry) bool {
		err = encodeKey(enc, key, e)
		return err == nil
	})
	if err != nil {
		return nil, err
	}
	if dump != nil {
		dump(func(args []string) {
			enc.byte(opModule)
			enc.uvarint(uint64(len(args)))
			for _, a := range args {
				enc.string(a)
			}
		})
	}
	enc.byte(opEOF)
	return appendCRC(enc.buf, crc64.Checksum(enc.buf[start:], crcTable)), nil
}

func appendCRC(b []byte, sum uint64) []byte {
	for i := 0; i < 8; i++ {
		b = append(b, byte(sum>>(8*i)))
	}
	return b
}

func encodeKey(w *encoder, key string, e *store.Entry) error {
	w.byte(opKey)
	w.uvarint(uint64(max(e.ExpireAt, 0)))
	w.byte(byte(e.Kind))
	w.string(key)
	switch e.Kind {
	case store.KindString:
		w.bytes(e.Str().Bytes())
	case store.KindList:
		items := e.List().Slice()
		w.uvarint(uint64(len(items)))
		for _, s := range items {
			w.string(s)
		}
	case store.KindHash:
		m := e.Hash().Map()
		w.uvarint(uint64(len(m)))
		for f, v := range m {
			w.string(f)
			w.string(v)
		}
	case store.KindSet:
		members := e.Set().Members()
		w.uvarint(uint64(len(members)))
		for _, m := range members {
			w.string(m)
		}
	case store.KindZSet:
		z := e.ZSet()
		w.uvarint(uint64(z.Len()))
		z.ForEach(func(p skiplist.Pair) bool {
			w.string(p.Member)
			w.float64(p.Score)
			return true
		})
	case store.KindJSON:
		w.string(e.JSON().String())
	case store.KindTimeSeries:
		s := e.Series()
		w.varint(s.Retention)
		w.uvarint(uint64(len(s.Labels)))
		for k, v := range s.Labels {
			w.string(k)
			w.string(v)
		}
		samples := s.Samples()
		w.uvarint(uint64(len(samples)))
		for _, smp := range samples {
			w.varint(smp.TS)
			w.float64(smp.Value)
		}
	case store.KindVector:
		x := e.Vectors()
		ids := x.IDs()
		w.uvarint(uint64(len(ids)))
		for _, id := range ids {
			v, attr, _ := x.Get(id)
			w.string(id)
			w.uvarint(uint64(len(v)))
			for _, f := range v {
				w.float32(f)
			}
			w.string(attr)
		}
	case store.KindBloom:
		w.bytes(e.Bloom().Marshal())
	case store.KindHLL:
		w.bytes(e.HLL().Marshal())
	default:
		return fmt.Errorf("snapshot: key %q has unknown kind %d", key, e.Kind)
	}
	return nil
}

// KeyEntry is one decoded key.
type KeyEntry struct {
	Key   string
	Entry *store.Entry
}

// Data is a fully decoded and checksum-verified snapshot, ready to apply.
type Data struct {
	Keys    []KeyEntry
	Modules [][]string
}

// Decode reads one snapshot from r and verifies its checksum. Strings of at
// least compressThreshold bytes are compressed (0 = never), as with
// DB.NewStringValue.
//
// If r is a *bufio.Reader, Decode consumes exactly the snapshot's bytes and
// leaves the rest unread (the AOF loader relies on this to continue with the
// commands that follow a snapshot preamble). Any other reader may be read past
// the end of the snapshot.
func Decode(r io.Reader, compressThreshold int) (*Data, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, 64<<10)
	}
	d := &decoder{r: br, crc: crc64.New(crcTable)}

	head := make([]byte, len(Magic)+1)
	if err := d.full(head); err != nil {
		if errors.As(err, new(*CorruptError)) && d.off == 0 {
			return nil, ErrBadMagic
		}
		return nil, err
	}
	if string(head[:len(Magic)]) != Magic {
		return nil, ErrBadMagic
	}
	if v := head[len(Magic)]; v != Version {
		return nil, fmt.Errorf("snapshot: unsupported format version %d (this build reads version %d)", v, Version)
	}

	data := &Data{}
	for {
		op, err := d.ReadByte()
		if err != nil {
			return nil, err
		}
		switch op {
		case opKey:
			k, err := decodeKey(d, compressThreshold)
			if err != nil {
				return nil, err
			}
			data.Keys = append(data.Keys, k)
		case opModule:
			argc, err := d.count()
			if err != nil {
				return nil, err
			}
			if argc == 0 {
				return nil, d.fail("empty module command")
			}
			args := make([]string, argc)
			for i := range args {
				if args[i], err = d.string(); err != nil {
					return nil, err
				}
			}
			data.Modules = append(data.Modules, args)
		case opEOF:
			want := d.crc.Sum64()
			var sum [8]byte
			if _, err := io.ReadFull(br, sum[:]); err != nil {
				return nil, d.fail("missing checksum after EOF record")
			}
			var got uint64
			for i := 7; i >= 0; i-- {
				got = got<<8 | uint64(sum[i])
			}
			if got != want {
				return nil, ErrChecksum
			}
			return data, nil
		default:
			return nil, d.fail("unknown record opcode 0x%02x", op)
		}
	}
}

func decodeKey(d *decoder, threshold int) (KeyEntry, error) {
	expireAt, err := d.uvarint()
	if err != nil {
		return KeyEntry{}, err
	}
	kindByte, err := d.ReadByte()
	if err != nil {
		return KeyEntry{}, err
	}
	key, err := d.string()
	if err != nil {
		return KeyEntry{}, err
	}
	kind := store.Kind(kindByte)
	v, err := decodeValue(d, kind, threshold)
	if err != nil {
		return KeyEntry{}, err
	}
	e := store.NewEntry(kind, v)
	e.ExpireAt = int64(expireAt)
	return KeyEntry{Key: key, Entry: e}, nil
}

type sizer interface{ MemSize() int64 }

func decodeValue(d *decoder, kind store.Kind, threshold int) (sizer, error) {
	switch kind {
	case store.KindString:
		b, err := d.bytes()
		if err != nil {
			return nil, err
		}
		return store.NewString(b, threshold), nil

	case store.KindList:
		n, err := d.count()
		if err != nil {
			return nil, err
		}
		l := deque.New()
		for range n {
			s, err := d.string()
			if err != nil {
				return nil, err
			}
			l.PushBack(s)
		}
		return l, nil

	case store.KindHash:
		n, err := d.count()
		if err != nil {
			return nil, err
		}
		h := store.NewHash()
		for range n {
			f, err := d.string()
			if err != nil {
				return nil, err
			}
			v, err := d.string()
			if err != nil {
				return nil, err
			}
			h.Set(f, v)
		}
		return h, nil

	case store.KindSet:
		n, err := d.count()
		if err != nil {
			return nil, err
		}
		s := store.NewSet()
		for range n {
			m, err := d.string()
			if err != nil {
				return nil, err
			}
			s.Add(m)
		}
		return s, nil

	case store.KindZSet:
		n, err := d.count()
		if err != nil {
			return nil, err
		}
		z := skiplist.New()
		for range n {
			m, err := d.string()
			if err != nil {
				return nil, err
			}
			score, err := d.float64()
			if err != nil {
				return nil, err
			}
			z.Add(m, score)
		}
		return z, nil

	case store.KindJSON:
		text, err := d.string()
		if err != nil {
			return nil, err
		}
		doc, err := jsondoc.Parse(text)
		if err != nil {
			return nil, d.fail("invalid JSON document: %v", err)
		}
		return doc, nil

	case store.KindTimeSeries:
		retention, err := d.varint()
		if err != nil {
			return nil, err
		}
		s := timeseries.New(retention)
		nl, err := d.count()
		if err != nil {
			return nil, err
		}
		for range nl {
			k, err := d.string()
			if err != nil {
				return nil, err
			}
			v, err := d.string()
			if err != nil {
				return nil, err
			}
			s.Labels[k] = v
		}
		ns, err := d.count()
		if err != nil {
			return nil, err
		}
		for range ns {
			ts, err := d.varint()
			if err != nil {
				return nil, err
			}
			v, err := d.float64()
			if err != nil {
				return nil, err
			}
			s.Add(ts, v)
		}
		return s, nil

	case store.KindBloom:
		return d.bloom()

	case store.KindHLL:
		return d.hll()

	case store.KindVector:
		n, err := d.count()
		if err != nil {
			return nil, err
		}
		x := vector.New()
		for range n {
			id, err := d.string()
			if err != nil {
				return nil, err
			}
			dim, err := d.count()
			if err != nil {
				return nil, err
			}
			if dim > maxString/4 {
				return nil, d.fail("vector dimension %d is too large", dim)
			}
			v := make([]float32, dim)
			for i := range v {
				if v[i], err = d.float32(); err != nil {
					return nil, err
				}
			}
			attr, err := d.string()
			if err != nil {
				return nil, err
			}
			if _, err := x.Add(id, v, attr); err != nil {
				return nil, d.fail("invalid vector %q: %v", id, err)
			}
		}
		return x, nil
	}
	return nil, d.fail("unknown value kind %d", kind)
}

// Apply inserts the decoded keys into e.DB (under LockAll; with flush the DB
// is emptied first, in the same critical section), then replays the module
// commands through the dispatcher as an internal master client. Keys that
// expired while the snapshot was on disk are skipped. It returns the first
// module command error, after replaying all of them.
func (data *Data) Apply(e *command.Engine, flush bool) error {
	now := time.Now().UnixMilli()
	unlock := e.DB.LockAll()
	if flush {
		e.DB.Flush()
	}
	for _, k := range data.Keys {
		if k.Entry.ExpireAt > 0 && k.Entry.ExpireAt <= now {
			continue
		}
		e.DB.Put(k.Key, k.Entry)
	}
	unlock()

	if len(data.Modules) == 0 {
		return nil
	}
	c := e.InternalClient("snapshot", true)
	var first error
	for _, args := range data.Modules {
		if v := e.Execute(c, args); v.IsError() && first == nil {
			first = fmt.Errorf("snapshot: replaying module command %q: %s", args[0], v.Str)
		}
	}
	return first
}

// Load decodes a snapshot from r into e.DB (existing keys are kept unless
// overwritten) and replays its module commands.
func Load(r io.Reader, e *command.Engine) error {
	data, err := Decode(r, int(e.DB.CompressThreshold.Load()))
	if err != nil {
		return err
	}
	return data.Apply(e, false)
}

// LoadFile loads the snapshot at path. A missing file returns an error
// satisfying errors.Is(err, fs.ErrNotExist).
func LoadFile(path string, e *command.Engine) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := Load(f, e); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// Bytes takes the locks, encodes and releases them; it returns the snapshot
// and the DB's dirty counter at the moment of the snapshot (pass it to
// DB.ResetDirty once the snapshot is safely stored).
func Bytes(e *command.Engine) (snap []byte, dirty int64, err error) {
	unlock := e.DB.LockAll()
	dirty = e.DB.Dirty()
	snap, err = AppendDB(nil, e.DB, e.DumpModules)
	unlock()
	return snap, dirty, err
}

// WriteFileAtomic writes data to path so that readers see either the old
// file or the complete new one, even after a crash: write a temp file in the
// same directory, fsync it, then rename it over path.
func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(name)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	ok = true
	SyncDir(dir)
	return nil
}

// SyncDir fsyncs a directory so a rename in it survives a power loss. It is
// best effort: Windows can't open directories for syncing.
func SyncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
