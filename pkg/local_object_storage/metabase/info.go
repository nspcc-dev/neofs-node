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
	return db.info
}
