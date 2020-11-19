package blobstor

import (
	"os"
)

// Info groups the information about BlobStor.
type Info struct {
	// Permission bits of the root directory.
	Permissions os.FileMode

	// Full path to the root directory.
	RootPath string
}

// DumpInfo returns information about the BlobStor.
func (b *BlobStor) DumpInfo() Info {
	return b.fsTree.Info
}
