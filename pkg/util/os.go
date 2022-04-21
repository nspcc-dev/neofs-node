package util

import "os"

// MkdirAllX calls os.MkdirAll with the passed permissions
// but with +x for a user and a group. This makes the created
// dir openable regardless of the passed permissions.
func MkdirAllX(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm|0110)
}
