package replication

// backlog keeps the most recent bytes of the replication stream so a replica
// that briefly lost its connection can continue with PSYNC instead of
// downloading a full snapshot again. It is created when the first replica
// attaches and then records every write, even while no replica is connected.
type backlog struct {
	buf  []byte // the last bytes of the stream; buf ends at the leader's offset
	size int    // how many bytes to keep
}

func newBacklog(size int) *backlog { return &backlog{size: size} }

func (b *backlog) append(p []byte) {
	b.buf = append(b.buf, p...)
	// Trim lazily (at 2x) so appends stay amortised O(len(p)).
	if len(b.buf) > 2*b.size {
		b.buf = append(make([]byte, 0, 2*b.size), b.buf[len(b.buf)-b.size:]...)
	}
}

// since returns a copy of the stream from offset `from` to `end` (the
// leader's current offset), or false if those bytes are no longer kept.
func (b *backlog) since(from, end int64) ([]byte, bool) {
	start := end - int64(len(b.buf))
	if from < start || from > end {
		return nil, false
	}
	return append([]byte(nil), b.buf[from-start:]...), true
}

// histLen is how many bytes are currently kept.
func (b *backlog) histLen() int { return min(len(b.buf), b.size) }
