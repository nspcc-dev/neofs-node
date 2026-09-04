//go:build !linux

package fstree

import "os"

func syncFS(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func newSpecificWriter(_ *FSTree) writer {
	return nil
}
