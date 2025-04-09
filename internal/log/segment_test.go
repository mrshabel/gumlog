package log

import (
	"io"
	"os"
	"testing"

	api "github.com/mrshabel/gumlog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	require.NoError(t, err)
	defer os.Remove(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// new segment with starting offset of 16 bytes
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	// verify next offset value
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		// append record
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		// read the appended record
		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// expect an end of file error since index is maxed out
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// expect index to be maxed
	require.True(t, s.IsMaxed())

	// update segment store and index capacity
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	// close segment and recreate it with the same index and store files
	err = s.Close()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// maxed store
	require.True(t, s.IsMaxed())

	// remove segment and recreate segment
	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
