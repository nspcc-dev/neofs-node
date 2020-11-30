package meta

import (
	"os"
)

// Info groups the information about DB.
type Info struct {
	// Full path to the metabase.
	Path string

	// Permission of database file.
	Permission os.FileMode
}

// DumpInfo returns information about the DB.
func (db *DB) DumpInfo() Info {
	return db.info
}
