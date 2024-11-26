/*
Package fstree implements a storage subsystem that saves objects as files in FS tree.

The main concept behind it is rather simple: each object is stored as a file
in a directory. Given that handling many files in the same directory is usually
problematic for file systems objects are being put into subdirectories by their
IDs. This directory tree can have different [FSTree.Depth] and each component
of the path (single directory name) is a [DirNameLen] number of ID bytes from
its string representation. File name then is a leftover of object ID (after
stripping [FSTree.Depth] bytes off of it) concatenated with container ID.

For example, an object with ID of WCdSV7F9TnDHFmbgKY7BNCbPs6g7meaVbh6DMNXbytB
from Hh6qJ2Fa9WzSK7PESResS4mmuoZ2a47Z7F6ZvmR7AEHU container will be stored
in W/C/d/S/V directory and a name of 7F9TnDHFmbgKY7BNCbPs6g7meaVbh6DMNXbytB.Hh6qJ2Fa9WzSK7PESResS4mmuoZ2a47Z7F6ZvmR7AEHU
if the depth is 5. The overall structure may look like this (a part of it):

	/W/
	├── 7
	│   └── 2
	│       └── F
	│           └── 6
	│               └── 32vhigDXRPkSwaCTw3FtxWbKjoDGoDvVTJqxBb.J4SkhNifjANvYGr56vh4NbGBRCxvk8PT9YE5EgFSb7cc
	├── C
	│   └── d
	│       └── S
	│           └── V
	│               └── 7F9TnDHFmbgKY7BNCbPs6g7meaVbh6DMNXbytB.Hh6qJ2Fa9WzSK7PESResS4mmuoZ2a47Z7F6ZvmR7AEHU

Binary file format can differ depending on the FSTree version. The basic format
that was used from the beginning is storing serialized protobuf representation
of the object as is. In this case file can be decoded into an object directly.
If compression is configured then the same file can have a compressed (ZSTD)
serialized protobuf.

Version 0.44.0 of the node has introduced a new file format that is used for
small files, the so-called "combined" one. It has a special prefix (0x7F) that
can not collide with protobuf-encoded or ZSTD-compressed data. Files using
this prefix can contain an unspecified number of objects (configured via
[WithCombinedCountLimit]) that all follow the same serialization pattern:
  - the first byte is magic 0x7F described above
  - 32-byte OID of the next object then
  - 4-byte BE integer length of the next object
  - followed by protobuf-encoded or ZSTD-compressed object data (of the length
    specified in the previous field)

Overall the structure is like this:

	[0x7F [OID A] [uint32 size]][object A][0x7F [OID B] [uint32 size]][object B]...

Even though this file contains several objects it has hard links for all of
them in the FS tree, so finding a file containing some object doesn't require
any additional effort. Finding it in the file contents however requires
reading the prefix described above, comparing the target OID and either
skipping the object length specified there or reading it after the prefix.
*/
package fstree
