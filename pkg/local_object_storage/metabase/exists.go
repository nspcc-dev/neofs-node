package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
)

// Exists checks if object is presented in metabase.
func (db *DB) Exists(addr *object.Address) (bool, error) {
	// FIXME: temp solution, avoid direct Get usage
	_, err := db.Get(addr)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}
