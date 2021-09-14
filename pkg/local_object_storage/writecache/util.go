package writecache

import (
	"os"
	"path"

	"go.etcd.io/bbolt"
)

// OpenDB opens BoltDB instance for write-cache. Opens in read-only mode if ro is true.
func OpenDB(p string, ro bool) (*bbolt.DB, error) {
	return bbolt.Open(path.Join(p, dbName), os.ModePerm, &bbolt.Options{
		NoFreelistSync: true,
		NoSync:         true,
		ReadOnly:       ro,
	})
}
