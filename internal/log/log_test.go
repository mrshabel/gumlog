package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	api "github.com/mrshabel/gumlog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// test for all cases of our log usage
func TestLog(t *testing.T) {
	table := map[string]func(t *testing.T, log *Log){
		"append and read record":      testAppendRead,
		"offset out of range error":   testOutOfRangeErr,
		"init with existing segments": testInitExisting,
		"reader":                      testReader,
		"truncate":                    testTruncate,
	}
	for scenario, fn := range table {
		t.Run(scenario, func(t *testing.T) {
			// create temp directory for each test case
			dir, err := os.MkdirTemp("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			config := Config{}
			config.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, config)
			require.NoError(t, err)

			// run test case
			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(record)
	require.NoError(t, err)
	// assert that offset is 0 since this is the first record
	require.Equal(t, uint64(0), off)

	// read value with offset and assert its correctness
	read, err := l.Read(off)
	require.NoError(t, err)
	require.Equal(t, record.Value, read.Value)

}

func testOutOfRangeErr(t *testing.T, l *Log) {
	// read offset that is out of range
	read, err := l.Read(1)
	require.Error(t, err)
	require.Nil(t, read)
}

func testInitExisting(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello world")}

	// append record 3 times before closing log
	for range 3 {
		_, err := l.Append(record)
		require.NoError(t, err)
	}
	// close log
	require.NoError(t, l.Close())

	// assert lowest and highest offsets
	off, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// create new log and assert that it is replayed
	n, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

// test that full log can be read as it is stored on disk
func testReader(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello world")}
	off, err := l.Append(record)
	require.NoError(t, err)
	// assert that offset is 0 since this is the first record
	require.Equal(t, uint64(0), off)

	// read full log
	reader := l.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)
	fmt.Printf("data bytes: %v\n", b)
	require.NoError(t, err)

	read := &api.Record{}
	// unmarshal data into record
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, record.Value, read.Value)
}

// test that unwanted log segments can be removed
func testTruncate(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello world")}
	for range 3 {
		_, err := l.Append(record)
		require.NoError(t, err)
	}
	// truncate log
	err := l.Truncate(1)
	require.NoError(t, err)

	// read truncated part
	_, err = l.Read(0)
	require.Error(t, err)
}
