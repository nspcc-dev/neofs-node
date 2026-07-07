//go:build !linux

package fstree

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

type fileOwner struct{}

func newFileOwner(string, fs.FileInfo) (fileOwner, error) {
	return fileOwner{}, nil
}

func rewriteCompressedObjectFile(path string, source fs.FileInfo, _ fileOwner, data []byte, noSync bool) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".rewrite-compressed-*")
	if err != nil {
		return fmt.Errorf("create temporary object file: %w", err)
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary object file %q: %w", tmpPath, err)
	}
	if err = tmp.Chmod(source.Mode().Perm()); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temporary object file %q: %w", tmpPath, err)
	}
	if !noSync {
		if err = tmp.Sync(); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("sync temporary object file %q: %w", tmpPath, err)
		}
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temporary object file %q: %w", tmpPath, err)
	}

	if err = replaceRewriteCompressedObjectFile(tmpPath, path, source); err != nil {
		return err
	}
	removeTmp = false
	return nil
}

// replaceRewriteCompressedObjectFile is only safe when no other process modifies
// the FSTree. Non-Linux users must stop the storage node before migration.
func replaceRewriteCompressedObjectFile(tmpPath, path string, source fs.FileInfo) error {
	current, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return errRewriteCompressedObjectGone
	}
	if err != nil {
		return fmt.Errorf("stat object file %q before replacement: %w", path, err)
	}
	if !os.SameFile(source, current) {
		return errRewriteCompressedObjectReplaced
	}
	if err = os.Rename(tmpPath, path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errRewriteCompressedObjectGone
		}
		return fmt.Errorf("replace object file %q: %w", path, err)
	}
	return nil
}

func checkRewriteCompressedOnlineSupport(string) error {
	return nil
}
