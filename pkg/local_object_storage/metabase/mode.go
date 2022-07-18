package meta

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// SetMode sets the metabase mode of operation.
// If the mode assumes no operation metabase, the database is closed.
func (db *DB) SetMode(m mode.Mode) error {
	db.modeMtx.Lock()
	defer db.modeMtx.Unlock()

	if db.mode == m {
		return nil
	}

	if !db.mode.NoMetabase() {
		if err := db.Close(); err != nil {
			return fmt.Errorf("can't set metabase mode (old=%s, new=%s): %w", db.mode, m, err)
		}
	}

	var err error
	switch {
	case m.NoMetabase():
		db.boltDB = nil
	case m.ReadOnly():
		err = db.Open(true)
	default:
		err = db.Open(false)
	}
	if err == nil && !m.NoMetabase() && !m.ReadOnly() {
		err = db.Init()
	}

	if err != nil {
		return fmt.Errorf("can't set metabase mode (old=%s, new=%s): %w", db.mode, m, err)
	}

	db.mode = m
	return nil
}
