//go:build linux

package fstree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"golang.org/x/sys/unix"
)

const (
	defaultTick        = 10 * time.Millisecond
	combinedSizeThresh = 128 * 1024
	combinedSizeLimit  = 8 * 1024 * 1024
	combinedCountLimit = 128
)

type linuxWriter struct {
	root   string
	perm   uint32
	flags  int
	bFlags int
	noSync bool

	batchLock sync.Mutex
	batch     *syncBatch
}

type syncBatch struct {
	lock     sync.Mutex
	fd       int
	procname string
	cnt      int
	size     int
	noSync   bool
	timer    *time.Timer
	ready    chan struct{}
	err      error
}

func newSpecificWriter(root string, perm fs.FileMode, noSync bool) writer {
	flags := unix.O_WRONLY | unix.O_TMPFILE | unix.O_CLOEXEC
	bFlags := flags
	if !noSync {
		flags |= unix.O_DSYNC
	}
	fd, err := unix.Open(root, flags, uint32(perm))
	if err != nil {
		return nil // Which means that OS-specific writeData can't be created and FSTree should use the generic one.
	}
	_ = unix.Close(fd) // Don't care about error.
	w := &linuxWriter{
		root:   root,
		perm:   uint32(perm),
		flags:  flags,
		bFlags: bFlags,
		noSync: noSync,
	}
	return w
}

func (w *linuxWriter) newSyncBatch() (*syncBatch, error) {
	fd, err := unix.Open(w.root, w.bFlags, w.perm)
	if err != nil {
		return nil, err
	}
	sb := &syncBatch{
		fd:       fd,
		procname: "/proc/self/fd/" + strconv.FormatUint(uint64(fd), 10),
		ready:    make(chan struct{}),
		noSync:   w.noSync,
	}
	sb.lock.Lock()
	sb.timer = time.AfterFunc(defaultTick, sb.sync)
	return sb, nil
}

func (b *syncBatch) sync() {
	b.lock.Lock()
	defer b.lock.Unlock()

	select {
	case <-b.ready:
		return
	default:
	}
	b.intSync()
}

func (b *syncBatch) intSync() {
	var err error

	if b.err == nil && !b.noSync {
		err = unix.Fdatasync(b.fd)
		if err != nil {
			b.err = err
		}
	}

	err = unix.Close(b.fd)
	if b.err == nil && err != nil {
		b.err = err
	}
	close(b.ready)
	_ = b.timer.Stop() // True is stopped, but false is "AfterFunc already running".
}

func (b *syncBatch) wait() error {
	<-b.ready
	return b.err
}

func (b *syncBatch) write(id oid.ID, p string, data []byte) error {
	var (
		err  error
		pref [1 + len(id) + 4]byte
	)
	pref[0] = combinedPrefix
	copy(pref[1:], id[:])
	binary.BigEndian.PutUint32(pref[1+len(id):], uint32(len(data)))

	n, err := unix.Writev(b.fd, [][]byte{pref[:], data})
	if err != nil {
		b.err = err
		b.intSync()
		return err
	}
	if n != len(pref)+len(data) {
		b.err = errors.New("incomplete write")
		b.intSync()
		return b.err
	}
	b.size += n
	b.cnt++
	err = unix.Linkat(unix.AT_FDCWD, b.procname, unix.AT_FDCWD, p, unix.AT_SYMLINK_FOLLOW)
	if err != nil {
		if errors.Is(err, unix.EEXIST) {
			// https://github.com/nspcc-dev/neofs-node/issues/2563
			return nil
		}
		b.err = err
		b.intSync()
		return b.err
	}
	return nil
}

func (w *linuxWriter) finalize() error {
	w.batchLock.Lock()
	defer w.batchLock.Unlock()
	if w.batch != nil {
		w.batch.sync()
		w.batch = nil
	}
	return nil
}

func (w *linuxWriter) writeData(id oid.ID, p string, data []byte) error {
	var err error
	if len(data) > combinedSizeThresh {
		err = w.writeFile(p, data)
	} else {
		err = w.writeCombinedFile(id, p, data)
	}
	if err != nil {
		if errors.Is(err, unix.ENOSPC) {
			return common.ErrNoSpace
		}
		return err
	}
	return nil
}

func (w *linuxWriter) writeCombinedFile(id oid.ID, p string, data []byte) error {
	var err error
	var sb *syncBatch

	w.batchLock.Lock()
	if w.batch == nil {
		w.batch, err = w.newSyncBatch()
		sb = w.batch
	} else {
		sb = w.batch
		sb.lock.Lock()
		select {
		case <-sb.ready:
			sb.lock.Unlock()
			w.batch, err = w.newSyncBatch()
			sb = w.batch
		default:
		}
	}
	if err != nil {
		return err
	}
	err = sb.write(id, p, data)
	if err == nil && sb.cnt >= combinedCountLimit || sb.size >= combinedSizeLimit {
		sb.intSync()
	}
	sb.lock.Unlock()
	w.batchLock.Unlock()
	if err != nil {
		return err
	}
	return sb.wait()
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
