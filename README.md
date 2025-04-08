# GUMLOG

A distributed commit log service.

## Storage Engine

The log engine consists of an underlying store and index for storing records in bytes. The storage contains record lines represented by the record length (8-byte) and actual data in bytes in `bigEndian`. The index contains mapping of keys to their respective record offsets in the store. This is done to ensure that lookups can be faster. The index file is memory-mapped to reduce calls made to the disk and improve read response, similar to how a read in memory would be.
Writes are moved into the file buffer when received, and flushed to the store on subsequent reads or storage closer. This however does not guarantee 99%+ durability as an ungraceful shutdown before a buffer flush could result in data loss. Further research and implementations will be made to ensure that the system is highly durable while maintaining the low latency guarantees, either through async buffer flushing by a background goroutine periodically or by enabling sync buffer flushing which makes sure every write goes to the underlying store before client acknowledgement is given.
Data checksums to validate data integrity is not implemented in this version yet.
