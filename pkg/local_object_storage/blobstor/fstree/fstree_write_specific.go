//go:build !linux

package fstree

import (
	"io/fs"
)

func newSpecificWriter(_ string, _ fs.FileMode, _ bool) writer {
	return nil
}
