//go:build linux

package fstree

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

type fileOwner struct {
	uid int
	gid int
}

func newFileOwner(path string, info fs.FileInfo) (fileOwner, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileOwner{}, fmt.Errorf("unsupported stat type for %q", path)
	}
	return fileOwner{uid: int(st.Uid), gid: int(st.Gid)}, nil
}

func rewriteCompressedObjectFile(path string, source fs.FileInfo, owner fileOwner, data []byte, noSync bool) error {
	dir := filepath.Dir(path)
	flags := unix.O_WRONLY | unix.O_TMPFILE | unix.O_CLOEXEC
	if !noSync {
		flags |= unix.O_DSYNC
	}
	fd, err := unix.Open(dir, flags, uint32(source.Mode().Perm()))
	if err != nil {
		return fmt.Errorf("open unnamed temporary object file: %w", err)
	}
	f := os.NewFile(uintptr(fd), "rewrite-compressed")
	defer f.Close()

	n, err := f.Write(data)
	if err != nil {
		return fmt.Errorf("write unnamed temporary object file: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("write unnamed temporary object file: %w", io.ErrShortWrite)
	}
	if err = f.Chmod(source.Mode().Perm()); err != nil {
		return fmt.Errorf("chmod unnamed temporary object file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat unnamed temporary object file: %w", err)
	}
	currentOwner, err := newFileOwner(path, info)
	if err != nil {
		return err
	}
	if currentOwner != owner {
		if err = f.Chown(owner.uid, owner.gid); err != nil {
			return fmt.Errorf("chown unnamed temporary object file: %w", err)
		}
	}
	linkPath := path + ".rewrite-compressed"
	_ = os.Remove(linkPath)
	linked := false
	defer func() {
		if linked {
			_ = os.Remove(linkPath)
		}
	}()
	procPath := "/proc/self/fd/" + strconv.Itoa(fd)
	if err = unix.Linkat(unix.AT_FDCWD, procPath, unix.AT_FDCWD, linkPath, unix.AT_SYMLINK_FOLLOW); err != nil {
		return fmt.Errorf("link unnamed temporary object file: %w", err)
	}
	linked = true
	return replaceRewriteCompressedObjectFile(linkPath, path)
}

func replaceRewriteCompressedObjectFile(tmpPath, path string) error {
	err := unix.Renameat2(unix.AT_FDCWD, tmpPath, unix.AT_FDCWD, path, unix.RENAME_EXCHANGE)
	if errors.Is(err, unix.ENOENT) {
		return errRewriteCompressedObjectGone
	}
	if err != nil {
		return fmt.Errorf("exchange rewritten object file %q: %w", path, err)
	}

	if err = os.Remove(tmpPath); err != nil {
		return fmt.Errorf("remove displaced object file %q: %w", path, err)
	}
	return nil
}

func checkRewriteCompressedOnlineSupport(root string) error {
	left, err := os.CreateTemp(root, ".rewrite-compressed-check-*")
	if err != nil {
		return fmt.Errorf("create rewrite compatibility file: %w", err)
	}
	leftPath := left.Name()
	defer os.Remove(leftPath)
	if err = left.Close(); err != nil {
		return fmt.Errorf("close rewrite compatibility file: %w", err)
	}

	right, err := os.CreateTemp(root, ".rewrite-compressed-check-*")
	if err != nil {
		return fmt.Errorf("create rewrite compatibility file: %w", err)
	}
	rightPath := right.Name()
	defer os.Remove(rightPath)
	if err = right.Close(); err != nil {
		return fmt.Errorf("close rewrite compatibility file: %w", err)
	}

	if err = unix.Renameat2(unix.AT_FDCWD, leftPath, unix.AT_FDCWD, rightPath, unix.RENAME_EXCHANGE); err != nil {
		return fmt.Errorf("online rewrite requires RENAME_EXCHANGE support: %w", err)
	}
	return nil
}
