//go:build linux

package fstree

import (
	"errors"
	"fmt"
	"io/fs"
	"strconv"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"golang.org/x/sys/unix"
)

type linuxWriter struct {
	root  string
	perm  uint32
	flags int
}

func newSpecificWriteData(root string, perm fs.FileMode, noSync bool) func(string, []byte) error {
	flags := unix.O_WRONLY | unix.O_TMPFILE | unix.O_CLOEXEC
	if !noSync {
		flags |= unix.O_DSYNC
	}
	fd, err := unix.Open(root, flags, uint32(perm))
	if err != nil {
		return nil // Which means that OS-specific writeData can't be created and FSTree should use the generic one.
	}
	_ = unix.Close(fd) // Don't care about error.
	w := &linuxWriter{
		root:  root,
		perm:  uint32(perm),
		flags: flags,
	}
	return w.writeData
}

func (w *linuxWriter) writeData(p string, data []byte) error {
	err := w.writeFile(p, data)
	if errors.Is(err, unix.ENOSPC) {
		return common.ErrNoSpace
	}
	return err
}

func (w *linuxWriter) writeFile(p string, data []byte) error {
	fd, err := unix.Open(w.root, w.flags, w.perm)
	if err != nil {
		return fmt.Errorf("unix open: %w", err)
	}
	tmpPath := "/proc/self/fd/" + strconv.FormatUint(uint64(fd), 10)
	n, err := unix.Write(fd, data)
	if err == nil {
		if n == len(data) {
			err = unix.Linkat(unix.AT_FDCWD, tmpPath, unix.AT_FDCWD, p, unix.AT_SYMLINK_FOLLOW)
			if errors.Is(err, unix.EEXIST) {
				// https://github.com/nspcc-dev/neofs-node/issues/2563
				err = nil
			}
		} else {
			err = errors.New("incomplete unix write")
		}
	}
	errClose := unix.Close(fd)
	if err != nil {
		return fmt.Errorf("unix write: %w", err) // Close() error is ignored, we have a better one.
	}
	if errClose != nil {
		return fmt.Errorf("unix close: %w", errClose)
	}
	return nil
}
