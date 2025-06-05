package log

import "github.com/hashicorp/raft"

// log configuration
type Config struct {
	// raft configuration
	Raft struct {
		raft.Config
		StreamLayer *raft.StreamLayer
		Bootstrap   bool
	}
	// maximum bytes for the store and index
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
