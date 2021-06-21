package meta

import (
	"fmt"
	"os"
	"path"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

// Open boltDB instance for metabase.
func (db *DB) Open() error {
	err := os.MkdirAll(path.Dir(db.info.Path), db.info.Permission)
	if err != nil {
		return fmt.Errorf("can't create dir %s for metabase: %w", db.info.Path, err)
	}

	db.log.Debug("created directory for Metabase", zap.String("path", db.info.Path))

	db.db, err = pebble.Open(db.info.Path, db.dbOptions)
	if err != nil {
		return fmt.Errorf("can't open boltDB database: %w", err)
	}

	db.log.Debug("opened boltDB instance for Metabase")

	return nil
}

// Init initializes metabase, however metabase doesn't need extra preparations,
// so it implemented to satisfy interface of storage engine components.
func (db *DB) Init() error {
	db.log.Debug("Metabase has been initialized")

	return nil
}

// Close closes boltDB instance.
func (db *DB) Close() error {
	return db.db.Close()
}
