package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/mrshabel/gumlog/api/v1"
)

// log to hold all segments and keep track of active segment
type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

// Creates a new log while defaulting the maximum store and index
// bytes to 1024 each
func NewLog(dir string, c Config) (*Log, error) {
	// setup defaults for values not specified
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{Dir: dir, Config: c}

	return l, l.setup()
}

// Setup then process new or existing segments in an order such that
// they are arranged from oldest to newest
func (l *Log) setup() error {
	// check for existing files
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// get the base offset for each segment since it's used in the filename
	// of store and index files
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// sort the base offsets
	sort.Slice(baseOffsets, func(i int, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		// create new segment with base offset for each entry
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// skip next element since baseOffset contains duplicates for
		// index and store files (same filename)
		i++
	}
	// new log for cases when no existing segments exist
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

// append a record to the active segment of a log and return the offset
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	// update active segment if maxed out
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// retrieve the record stored at a given offset with the segment's offset
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// find segment containing record with the offset
	// offset should be between baseOffset of segment and
	// nextOffset of the same segment
	var s *segment
	// TODO: use binary search
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}

	// return segment data
	return s.Read(off)
}

// close all segments in the log
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// remove log by closing it and deleting all related records
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// reset log by removing it and setting it up again
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

// retrieve the lowest segment offset in the log
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// retrieve the highest segment offset in the log
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// get the last segment's offset
	off := l.segments[len(l.segments)-1].nextOffset
	// empty segments
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// remove old segments from disk to avoid overflow
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		// discard segments whose highest offsets are lesser than lower
		if s.nextOffset-1 <= lowest {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	// update segments in-place
	l.segments = segments
	return nil
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	// read content of store from offset
	n, err := o.ReadAt(p, o.off)
	// EOF may be returned in cases where the allocated byte slice exceeds data read
	if err != nil && err != io.EOF {
		return 0, err
	}
	o.off += int64(n)
	return n, err
}

// read the entire log with all segments.
// this concatenates all segments and read them as one
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		// add segment reader that implements Reader interface
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

// create a new segment with a given base offset and set it as the
// active segment
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	// set it as the active segment
	l.activeSegment = s
	return nil
}
