pydedupfs
=========

python-fuse deduplication filesystem

started as an experiment with deduplication an fuse

fist working version, for testing.
This version deletes the database at every start.

first finding:
blocks are store in filesystem with digest as name
blocks database is gdbm sqlite is to slow for hashmap
