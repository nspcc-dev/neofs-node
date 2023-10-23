//go:build !linux

package fstree

func newSpecificWriteData(_ string, _ fs.FileMode, _ bool) func(string, []byte) error {
	return nil
}
