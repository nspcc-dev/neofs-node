//go:build linux

package fstree

import (
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"golang.org/x/sys/unix"
)

const (
	defaultTick        = 20 * time.Millisecond
	combinedSizeThresh = 16 * 1024 * 1024
	combinedSizeLimit  = 128 * 1024 * 1024
	combinedCountLimit = 128
)

type osSpecific struct {
	// FD for syncfs()
	batchLock sync.Mutex
	batch     *syncBatch
}

type syncBatch struct {
	lock     sync.Mutex
	fd       int
	procname string
	cnt      int
	size     int
	timer    *time.Timer
	ready    chan struct{}
	err      error
}

func newSyncBatch(root string, perm uint32) (*syncBatch, error) {
	flags := unix.O_WRONLY | unix.O_TMPFILE | unix.O_CLOEXEC
	fd, err := unix.Open(root, flags, perm)
	if err != nil {
		return nil, err
	}
	sb := &syncBatch{
		fd:       fd,
		procname: "/proc/self/fd/" + strconv.FormatUint(uint64(fd), 10),
		ready:    make(chan struct{}),
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

	if b.err == nil {
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
	var err error

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

	var pref [1 + len(id) + 4]byte
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
	return nil
}

func (t *FSTree) initOSSpecific() error {
	return nil
}

func (t *FSTree) closeOSSpecific() error {
	t.batchLock.Lock()
	defer t.batchLock.Unlock()
	if t.batch != nil {
		t.batch.sync()
		t.batch = nil
	}
	return nil
}

func (t *FSTree) writeData(id oid.ID, p string, data []byte) error {
	var err error
	if len(data) > combinedSizeThresh {
		err = t.writeFile(p, data)
	} else {
		err = t.writeCombinedFile(id, p, data)
	}
	if err != nil {
		if errors.Is(err, unix.ENOSPC) {
			return common.ErrNoSpace
		}
		return err
	}
	return nil
}

func (t *FSTree) writeCombinedFile(id oid.ID, p string, data []byte) error {
	var err error
	var sb *syncBatch

	t.batchLock.Lock()
	if t.batch == nil {
		t.batch, err = newSyncBatch(t.RootPath, uint32(t.Permissions))
		sb = t.batch
	} else {
		sb = t.batch
		sb.lock.Lock()
		select {
		case <-sb.ready:
			sb.lock.Unlock()
			t.batch, err = newSyncBatch(t.RootPath, uint32(t.Permissions))
			sb = t.batch
		default:
		}
	}
	if err != nil {
		t.batchLock.Unlock()
		return err
	}
	err = sb.write(id, p, data)
	if err == nil && sb.cnt >= combinedCountLimit || sb.size >= combinedSizeLimit {
		if sb.timer.Stop() {
			go sb.intSync()
		}
	}
	sb.lock.Unlock()
	t.batchLock.Unlock()
	if err != nil {
		return err
	}
	return sb.wait()
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
