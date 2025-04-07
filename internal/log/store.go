// this package contains an implementation of a log store: a file that
// keeps records in
package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// encoding for persisting record sizes and index entries
	enc = binary.BigEndian
)

const (
	// number of bytes used to store record length
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// create a new store from a given file. file could be new or existing
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// get the file size
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// append a record to the underlying store.
// returns the number of bytes written, position of record in the store, error
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// get the underlying store size
	pos = s.size
	// write record length to buffer in binary format
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// write actual data to buffer. record now becomes: `length-data`
	// length of every record is prefixed is used as prefix for its data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// update store size for next operation
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// read a record from the underlying store with its position
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// flush existing data on buffer
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// read prefixed length of current data needed
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// read record by using its initial position and standard length as offset
	// this will skip the prefixed length and only read the actual data
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// read len(p) bytes into p beginning at off offset
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// persist buffered data before closing the underlying file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
