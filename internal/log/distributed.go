package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github.com/mrshabel/gumlog/api/v1"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

// fsm is the finite-state machine that is responsible for handling all business logic for the internal log.
type fsm struct {
	log *Log
}

// NewDistributedLog sets up a new instance of a distributed log which achieves consensus with raft
func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{config: config}

	// setup log and raft server
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

// setupLog creates a log for this server
func (l *DistributedLog) setupLog(dataDir string) error {
	// create log directory with necessary permissions
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	// setup internal log
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	// setup finite-state machine
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	// setup internal log with offset of 1
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// setup stable store to keep cluster configuration and metadata
	storePath := filepath.Join(dataDir, "raft", "stable")
	stableStore, err := raftboltdb.NewBoltStore(storePath)
	if err != nil {
		return err
	}

	// setup snapshot store to hold snapshotted data. this will include everything in the raft data directory
	snapshotPath := filepath.Join(dataDir, "raft")
	maxSnapshotRetained := 1
	snapshotStore, err := raft.NewFileSnapshotStore(snapshotPath, maxSnapshotRetained, os.Stderr)
	if err != nil {
		return err
	}

	// setup transport for peer communication
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		*l.config.Raft.StreamLayer, maxPool, timeout, os.Stderr,
	)

	// setup raft configuration
	config := raft.DefaultConfig()
	// assign unique mandatory node ID to the server
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	// create raft instance
	l.raft, err = raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{ID: config.LocalID, Address: transport.LocalAddr()}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Append adds a new record to the distributed log
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	// apply write to the raft fsm
	res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}
	// cast interface response as a log record
	return res.(*api.ProduceResponse).Offset, nil
}

// apply wraps Raft Apply API and is used to inform the fsm to append a record to the log
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	// write req type (append) and message to buffer slice
	var buf bytes.Buffer
	if _, err := buf.Write([]byte{byte(reqType)}); err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err = buf.Write(b); err != nil {
		return nil, err
	}

	// apply command to raft fsm. this replicates the record and appends it to the leader's log
	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	// check for raft errors, (timeouts...)
	if future.Error() != nil {
		return nil, future.Error()
	}
	// get response
	res := future.Response()
	// check if a service error was returned in the process
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

// Read reads a record for the given offset from the server's log. This uses a "relaxed consistency" since reads does not go through raft here
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// enfore raft.FSM behavior on the internal fsm defined
var _ raft.FSM = (*fsm)(nil)

// request types for the distributed log service
type RequestType uint8

const (
	AppendRequestType RequestType = iota
)

// Apply is invoked internally by raft after a log entry is committed
func (l *fsm) Apply(record *raft.Log) interface{} {
	// extract the data from the raft log
	buf := record.Data

	// get the request type
	reqType := RequestType(buf[0])
	switch reqType {
	// handle append requests
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	// unmarshal the byte slice into a protobuf and append to the internal log
	var req api.ProduceRequest
	if err := proto.Unmarshal(b, &req); err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// snapshotting
type snapshot struct {
	reader io.Reader
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

// Snapshot creates and returns a point-in-time snapshot of the FSM state
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// get entire log state
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

// Persist writes the FSM state to the underlying sink, a file in this case
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	// write snapshotted data from log to the raft sink
	if _, err := io.Copy(sink, s.reader); err != nil {
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

// Restore restores an FSM from a snapshot
func (f *fsm) Restore(r io.ReadCloser) error {
	// get record length
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		// use the record length as the initial offset for the first record
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

// log store
type logStore struct {
	*Log
}

var _ raft.LogStore = (*logStore)(nil)

func newLogStore(dir string, cfg Config) (*logStore, error) {
	log, err := NewLog(dir, cfg)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

// GetLog retrieves a record at a given index
func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	// retrieve the data at a given index
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

// Delete old records
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

// stream layer

// StreamLayer is an abstraction to connect with Raft servers through an encrypted channel
type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

func NewStreamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln: ln, serverTLSConfig: serverTLSConfig, peerTLSConfig: peerTLSConfig,
	}
}

const RaftRPC = 1

// Dial makes outgoing connections to other servers in the Raft cluster
func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// write a single byte on connection as a way of identifying multiplexed requests
	if _, err = conn.Write([]byte{byte(RaftRPC)}); err != nil {
		return nil, err
	}

	// setup peer tls on connection if provided
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

// Accept is simply an implementation on the net.Listener interface that indicates what to do when a request is received
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	// check if connection is multiplexed (raft + grpc)
	b := make([]byte, 1)
	if _, err = conn.Read(b); err != nil {
		return nil, err
	}
	if bytes.Compare(b, []byte{byte(RaftRPC)}) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}

	// setup tls
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}
