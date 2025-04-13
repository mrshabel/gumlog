# GUMLOG

A distributed commit log service.

## Storage Engine

The log engine consists of segment(s) which holds an underlying store and index for storing records in bytes. The storage contains record lines represented by the record length (8-byte) and actual data in bytes in `bigEndian`. The index contains mapping of keys to their respective record offsets in the store. This is done to ensure that lookups can be faster. The index file is memory-mapped to reduce calls made to the disk and improve read response, similar to how a read in memory would be.
Writes are moved into the file buffer when received, and flushed to the store on subsequent reads or storage closer. This however does not guarantee 99%+ durability as an ungraceful shutdown before a buffer flush could result in data loss. Further research and implementations will be made to ensure that the system is highly durable while maintaining the low latency guarantees, either through async buffer flushing by a background goroutine periodically or by enabling sync buffer flushing which makes sure every write goes to the underlying store before client acknowledgement is given.
Data checksums to validate data integrity is not implemented in this version yet.

### Store

At the heart of the log engine is a store responsible for storing the encoded data on disk. The store uses a buffer to batch up writes and reduce I/O calls to the OS. A write is composed of the form `[record length (stored as 8-byte)][record data in bytes]` and written to the file buffer. Whenever a read is made, the buffer is flushed to disk first before the record is retrieved. An Append essentially writes the binary data and return the record's position in the store. The position starts at 0, and moves in increments of the format discussed earlier (record length, record). Any Read uses the position returned from the appended record to access the exact position of the record on disk. This is done to avoid sequential scan of all records. To optimize data reading from the store, indexes are introduced as a way of mapping record offset values to their respective positions on disk.

### Index

The index files are an abstraction of a memory-mapped file. Since the data stored is minimal, a link to the file on disk is created in memory to speed up record position lookups. Memory-mapped files restrict truncation and resizing so the file is initially padded with 0's to the maximum index bytes as specified in the configuration. A write consists of the relative record offset and its position on disk `[offset(4-byte). value starts from 0, 1...][position in store(8-byte)]`. The index can be read with a negative index where data is retrieved with its output offset returned.

### Segment

Segment is the smallest unit of the log and contains pointers to a store and index file. Segments are identified with their base offsets (0, 1...) which makes it easy to sort them and efficiently retrieve stored records. The base offset of segments allows the offset values of index file records to be presented in their relative formats (relative to the base offset), hence reducing storage space for storing record offsets (uint64 to uint32). A read on a segment essentially checks for the record position from the index with the given offset, then read the record's value from the store with the position found.

### Log

The log holds multiple segments which are logically related and can be queried as a unit. Only one segment in the log can be active at a time for writes. The setup of a log (new or existing) ensures that all associated segments data are either replayed or created with their appropriate max sizes from the configuration. A write will append to the active segment first then update the segment with an new offset (old offset + 1) if the current segment is maxed out. Records can be read with their offset values. Stale records will be cleared periodically to avoid maxing storage capacity. All segments in the log can be read as if they were a single record. This allows for easy data export to different nodes.
