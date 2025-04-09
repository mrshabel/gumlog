package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/mrshabel/gumlog/api/v1"
	"google.golang.org/protobuf/proto"
)

// segment struct to hold store and index
type segment struct {
	store *store
	index *index
	// starting offset of this segment
	baseOffset uint64
	// next available offset for appending
	nextOffset uint64
	config     Config
}

// create a new instance of a segment
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	// create/open file in append mode
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		return nil, err
	}

	// create instance of store and index file
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// get next offset value. this attempts to retrieve the last entry in the
	// index if present
	if off, _, err := s.index.Read(-1); err != nil {
		// empty index
		s.nextOffset = baseOffset
	} else {
		// index with at least an element. nextOffset will be next position
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// append a new record to the segment
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// get offset to append data
	cur := s.nextOffset
	record.Offset = cur

	// marshal the record into a byte slice
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// append record to store and track its index
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	// use offset relative to the base offset
	if err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos); err != nil {
		return 0, err
	}
	// update next offset
	s.nextOffset++
	return cur, nil
}

// read the a record with its absolute offset
func (s *segment) Read(off uint64) (*api.Record, error) {
	// retrieve the record position from the index and lookup its value from the store

	// convert absolute index offset to relative offset for index
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// unmarshal byte slice into protobuf
	record := &api.Record{}
	err = proto.Unmarshal(p, record)

	return record, err
}

// check whether a segment has reached its maximum size or not.
// the segment is maxed if its underlying store or index size has reached its
// max bytes as specified in the configuration
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// remove the segment and its associated store and index files
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// close the segment's store and index files
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// find the nearest multiple of k less than or equal to
// j to ensure that all operations
// and storage used stays under user's disk capacity
func (s *segment) nearestMultiple(j, k uint64) uint64 {
	// floor the division to avoid multiplier cancellation
	return (j / k) * k
}
