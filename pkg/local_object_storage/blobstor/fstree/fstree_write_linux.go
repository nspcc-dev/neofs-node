//go:build linux

package fstree

import (
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"golang.org/x/sys/unix"
)

const defaultTick = 20 * time.Millisecond

type osSpecific struct {
	// FD for syncfs()
	treeFD   int
	batch    atomic.Pointer[syncBatch]
	interval time.Duration
	stop     chan struct{}
	stopped  chan struct{}
}

type syncBatch struct {
	ready chan struct{}
}

func newSyncBatch() *syncBatch {
	return &syncBatch{
		ready: make(chan struct{}),
	}
}

func (b *syncBatch) sync(fd int) {
	_ = unix.Syncfs(fd) // FD is OK, this can't fail.
	close(b.ready)
}

func (b *syncBatch) wait() {
	<-b.ready
}

func (t *FSTree) initOSSpecific() error {
	if t.noSync {
		return nil
	}
	var err error

	t.stop = make(chan struct{})
	t.stopped = make(chan struct{})
	t.interval = defaultTick
	t.batch.Store(newSyncBatch())

	t.treeFD, err = unix.Open(t.RootPath, unix.O_WRONLY|unix.O_TMPFILE|unix.O_CLOEXEC, uint32(t.Permissions))
	if err != nil {
		return err
	}
	go t.flusher()
	return nil
}

func (t *FSTree) flusher() {
	var tick = time.NewTicker(t.interval)
flushloop:
	for {
		select {
		case <-tick.C:
			var b = t.batch.Swap(newSyncBatch())

			st := time.Now()
			b.sync(t.treeFD)

			interval := t.interval - time.Since(st)
			if interval <= 0 {
				interval = time.Microsecond
			}
			tick.Reset(interval)
		case <-t.stop:
			var b = t.batch.Swap(nil)
			b.sync(t.treeFD)
			break flushloop
		}
	}
	close(t.stopped)
}

func (t *FSTree) closeOSSpecific() error {
	if t.noSync || t.stop == nil {
		return nil
	}
	select {
	case <-t.stopped:
		return nil
	default:
	}
	close(t.stop)
	<-t.stopped
	_ = unix.Close(t.treeFD)
	return nil
}

func (t *FSTree) writeData(p string, data []byte) error {
	err := t.writeFile(p, data)
	if err != nil {
		if errors.Is(err, unix.ENOSPC) {
			return common.ErrNoSpace
		}
		return err
	}
	var b = t.batch.Load()
	if b == nil {
		return errors.New("fstree is closed")
	}
	b.wait()
	return nil
}

func (t *FSTree) writeFile(p string, data []byte) error {
	flags := unix.O_WRONLY | unix.O_TMPFILE | unix.O_CLOEXEC
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
