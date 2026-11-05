// Package compression compresses large string values in memory (DEFLATE,
// fastest level) so big cached payloads like HTML or JSON take less RAM.
package compression

import (
	"bytes"
	"compress/flate"
	"io"
	"sync"
)

var writers = sync.Pool{New: func() any {
	w, _ := flate.NewWriter(nil, flate.BestSpeed)
	return w
}}

// Compress returns the compressed bytes, or nil if compression didn't save at
// least 10%, in which case the caller should store the original.
func Compress(b []byte) []byte {
	var buf bytes.Buffer
	w := writers.Get().(*flate.Writer)
	w.Reset(&buf)
	w.Write(b)
	w.Close()
	writers.Put(w)
	if buf.Len() > len(b)*9/10 {
		return nil
	}
	return buf.Bytes()
}

// Decompress inflates data produced by Compress. size is the original length.
func Decompress(data []byte, size int) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	out := make([]byte, size)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}
