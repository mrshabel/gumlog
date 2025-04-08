package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	// temp index directory for testing
	f, err := os.CreateTemp("", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// set initial segment index bytes to 1024
	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	// read last record in index
	_, _, err = idx.Read(-1)
	// expect an EOF error as data is read from "empty" index
	require.Error(t, err)
	require.Equal(t, idx.Name(), f.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
	for _, want := range entries {
		// write record and read its value
		err := idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, pos, want.Pos)
	}

	// read past existing entries (non-existent record)
	_, _, err = idx.Read(int64(len(entries)))
	// expect an EOF error as data is read from non-existent offset
	require.Error(t, io.EOF, err)

	// close file and rebuild index from existing file
	err = idx.Close()
	require.NoError(t, err)

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, off, entries[1].Off)
	require.Equal(t, pos, entries[1].Pos)
}
