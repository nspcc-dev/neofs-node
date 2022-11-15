package meta

import (
	"io/fs"
)

// Info groups the information about DB.
type Info struct {
	// Full path to the metabase.
	Path string

	// Permission of database file.
	Permission fs.FileMode
}

// DumpInfo returns information about the DB.
func (db *DB) DumpInfo() Info {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	return db.info
}
