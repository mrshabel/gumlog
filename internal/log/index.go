package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// offset width for index in bytes
	offWidth uint64 = 4
	// width of record's position in the store
	posWidth uint64 = 8
	// track start position of next entry
	entWidth = offWidth + posWidth
)

type index struct {
	// persisted file
	file *os.File
	// memory mapped file for faster reads
	mmap gommap.MMap
	// size of index and location where next write will be appended
	size uint64
}

// create a new instance of the index file
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	// get file info
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// grow file to maximum index size before memory mapping as
	// file can't be grown after memory mapping. this pads the file
	// with zero's until the size is full
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// create memory mapping
	mmap, err := gommap.Map(
		idx.file.Fd(),
		// assign rw permissions
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}
	idx.mmap = mmap

	return idx, nil
}

func (i *index) Name() string {
	return i.file.Name()
}

// find the associated record's position in the store with its offset value
// 'in' is a relative offset
// returns the output relative offset, position of record in the store, error
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	// check for negative offset values since 'in' is a relative measurement
	if in == -1 {
		out = uint32(i.size/entWidth - 1)
	} else {
		out = uint32(in)
	}
	// get byte position of entry in memory-mapped file
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// extract the actual content from the file
	// first 4 bytes is the offset and remaining 8 bytes for actual position
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// append a given relative offset value and actual position to index file
func (i *index) Write(off uint32, pos uint64) error {
	// check if there is enough space for writes
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// add to the end of the index. offsets are relative to the base(first) offset
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)

	return nil
}

func (i *index) Close() error {
	// flush changes made to the memory mapped region synchronously to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// commit file content to disk
	if err := i.file.Sync(); err != nil {
		return err
	}

	// unmap file before truncating. without this, windows prevents the
	// truncation as it may lead to corrupt memory
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}
	// truncate file to actual size to compensate for file growth during
	// memory mapping. this removes all zero padding
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	// close file
	if err := i.file.Close(); err != nil {
		return err
	}
	return nil
}
