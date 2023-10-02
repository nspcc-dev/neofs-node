package writecache

import (
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

// OpenDB opens BoltDB instance for write-cache. Opens in read-only mode if ro is true.
func OpenDB(p string, ro bool) (*bbolt.DB, error) {
	return bbolt.Open(filepath.Join(p, dbName), os.ModePerm, &bbolt.Options{
		NoFreelistSync: true,
		ReadOnly:       ro,
		Timeout:        time.Second,
	})
}
