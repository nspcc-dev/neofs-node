//go:build !linux

package fstree

func newSpecificWriter(_ *FSTree) writer {
	return nil
}
