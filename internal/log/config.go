package log

// log configuration
type Config struct {
	// maximum bytes for the store and index
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
