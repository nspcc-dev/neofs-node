//go:build !linux

package fstree

import (
	"io/fs"
)

func newSpecificWriteData(_ string, _ fs.FileMode, _ bool) func(string, []byte) error {
	return nil
}
