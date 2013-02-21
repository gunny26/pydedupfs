pydedupfs
=========

python-fuse deduplication filesystem

A simple deduplicating fuse based filesystem with very limited memory requirements.

This version is for beta production

Requirements:
- fuse
- hashlib - builtin python
- cPickle - builtin python
- gdbm - use to store blockdigest

Concept:

PyDedupFS deduplicate Blocks of data at a given fixed length (default=128k).
These blocks are hashed with help from hashlib (default hashlib.sha1).
For every file there is also a whole file digest with with the same hashin function.
(So you can verify the stored file, with this original file digest)

PyDedupfs is designed to be simple and uses as much features
from the underlying filesystem. Blocks are stored as ordinary
files with hexdigest as name.

According to this, a file stored in PyDedupFS is stored with multiple files:
- one file for original file information and meta data
- (size of file / blocksize) files for stored blocks

Files are stored in the real filesystem, but the content of the file is 
cPickled information how to assemble the original data,
a digest over the whole file an stat structure.
( a python tuple (digest, st, sequence) )
You get this Information simple by reading this file with cPickle.load

The only database is gdbm to store blockhash to reference counter.
The reference counter is necessary for delete operations,
to delete only unused blocks, and to find existing blocks.


Diffent to other deduplicating Filesystems:

- does not use huge amounts of memory

- does not use a database for file structure
  The files reside in the real filesystem, but they are
  only filled with information how to assemble the real data.
  Filesystems do that for a long time, why implement a
  filesystem structure in a RDBMS -> it will be slower in the most cases.
  ( tested in experimental branch )

- does not use a database for block storage as blob
  filesystems can store data better than databases
  database overhead is significant
  ( tested in experimental branch )

- disk based block digest dictionary, based on gdbm
  it is robust and standard


Architecure:

PyDeduFS creates 3 Directories under <BASE>

- <BASE>/meta
  In this directory the gdbm database for block digest and reference counter is stored

- <BASE>/files
  Original files filled with assemling information

- <BASE>/blocks
  blocks of data with hexdigest as name


How it works:

a short explanation how PyDedupFS works

Read from File XYZ.txt

- get real filename for XYZ.txt under <Base>  directory
- get information out of <Base>/XYZ.txt with cPickle.load()
- get sequence of blocks to assemble data
- add 0x00 EOF

Write file XYZ.txt

- split file inline into 128k Blocks
- store Block if not exists and store digest Blockdigest dictionary
- store file information - digest of whole file, stat struct, and sequence
  in <BASE>/XYZ.txt


Usage:

Download via github

PyDeduFs.py --base=<Path to real data> <fuse Options> <Mountpoint>

So PyDedupFs will go in background and will log to /tmp/pydedupfs.log

Non Fuse options:

--base = base directory to store real data
--hashfunc = hashing function of hashlib to use ( sha1, md5, sha256 ... ), default "sha1"
             dont change this after first use of filesystem !
--blocksize = blocksize to split data in, default 128k

Fuse options set in program:

multipathin off
direct-io off

Loggin:

logging can be adjusted in logging.conf


Filesystem Feature:

implements standard fuse method, but
- symlink - very hard to implement with conecpt of PyDedupFS
- readlink - no symlink, no readlink
- truncate
- ftruncate
