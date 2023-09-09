//go:build linux

package fstree

import (
	"errors"
	"strconv"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"golang.org/x/sys/unix"
)

func (t *FSTree) writeData(p string, data []byte) error {
	err := t.writeFile(p, data)
	if errors.Is(err, unix.ENOSPC) {
		return common.ErrNoSpace
	}
	return err
}

func (t *FSTree) writeFile(p string, data []byte) error {
	flags := unix.O_WRONLY | unix.O_TMPFILE | unix.O_CLOEXEC
	if !t.noSync {
		flags |= unix.O_DSYNC
	}
	fd, err := unix.Open(t.RootPath, flags, uint32(t.Permissions))
	if err != nil {
		return err
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
			err = errors.New("incomplete write")
		}
	}
	errClose := unix.Close(fd)
	if err != nil {
		return err // Close() error is ignored, we have a better one.
	}
	return errClose
}
