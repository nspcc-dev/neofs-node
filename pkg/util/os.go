package util

import "os"

// MkdirAllX calls os.MkdirAll with passed permissions
// but with +x for user and group. This makes created
// dir openable regardless of the passed permissions.
func MkdirAllX(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm|0110)
}
