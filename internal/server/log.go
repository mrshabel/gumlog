package server

import (
	"fmt"
	"sync"
)

// a representation of data record in the append-only log
type Record struct {
	Value []byte `json:"value"`
	// a positive value offset from 0-2^64-1
	Offset uint64 `json:"offset"`
}

// an append-only log
type Log struct {
	mu      sync.Mutex
	records []Record
}

var (
	ErrOffsetNotFound = fmt.Errorf("offset not found")
)

func NewLog() *Log {
	return &Log{}
}

// append a record to the tail of the log
// returns the offset position of the appended record
func (l *Log) Append(record Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// get the current offset
	record.Offset = uint64(len(l.records))
	l.records = append(l.records, record)
	return record.Offset, nil
}

// read a record from a log file
func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// check if offset is valid
	if offset >= uint64(len(l.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return l.records[offset], nil
}
