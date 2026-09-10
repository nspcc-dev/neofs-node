package fstree

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"syscall"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const genericFileWriteRetryCount = 5

type genericWriter struct {
	perm  fs.FileMode
	flags int
}

func newGenericWriter(perm fs.FileMode, noSync bool) writer {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_EXCL
	if !noSync {
		flags |= os.O_SYNC
	}
	var w = &genericWriter{
		perm:  perm,
		flags: flags,
	}
	return w
}

func (w *genericWriter) finalize() error {
	return nil
}

func (w *genericWriter) writeBatch(objs []writeDataUnit) error {
	for _, obj := range objs {
		err := w.writeData(obj.id, obj.path, obj.data)
		if err != nil {
			return err
		}
	}
	return nil
}

type genericFileWriteStream struct {
	genericWriter *genericWriter
	targetPath    string
	tryIdx        int
	tmpPath       string
	tmpFile       *os.File
	isWritten     bool
	aborted       bool
}

var errStreamAborted = errors.New("stream already aborted")

func newGenericFileWriteStream(w *genericWriter, targetPath string) *genericFileWriteStream {
	return &genericFileWriteStream{
		genericWriter: w,
		targetPath:    targetPath,
	}
}

func (x *genericFileWriteStream) abort() {
	if x.aborted {
		return
	}
	x.aborted = true
	if x.tmpFile != nil {
		_ = x.tmpFile.Close()
		x.tmpFile = nil
	}
	_ = os.RemoveAll(x.tmpPath)
}

func (x *genericFileWriteStream) handleAlreadyExists() error {
	if x.isWritten || x.tryIdx >= genericFileWriteRetryCount-1 {
		return syscall.EEXIST
	}
	x.tryIdx++
	return nil
}

func (x *genericFileWriteStream) handleFileError(err error) error {
	err = handleFileError(x.tmpPath, err)
	if errors.Is(err, syscall.EEXIST) {
		return x.handleAlreadyExists()
	}
	if errors.Is(err, common.ErrNoSpace) {
		x.abort()
	}
	return err
}

func (x *genericFileWriteStream) openTmpFile() error {
	for {
		x.tmpPath = newFilePathForTry(x.targetPath, x.tryIdx)

		var err error
		x.tmpFile, err = x.genericWriter.openFile(x.tmpPath)
		if err == nil {
			return nil
		}

		err = x.handleFileError(err)
		if err != nil {
			return err
		}
	}
}

func (x *genericFileWriteStream) openTmpFileIfNeeded() error {
	if x.tmpFile != nil {
		return nil
	}
	return x.openTmpFile()
}

func (x *genericFileWriteStream) Write(p []byte) (int, error) {
	if x.aborted {
		return 0, errStreamAborted
	}

	for {
		err := x.openTmpFileIfNeeded()
		if err != nil {
			return 0, err
		}

		n, err := writeToFile(x.tmpFile, p)
		if err != nil {
			err = x.handleFileError(err)
			if err == nil {
				continue
			}
			return n, err
		}

		if n > 0 {
			x.isWritten = true
		}

		return n, nil
	}
}

func (x *genericFileWriteStream) Close() error {
	if x.aborted {
		return errStreamAborted
	}

	for {
		err := x.openTmpFileIfNeeded()
		if err != nil {
			return err
		}

		err = closeFile(x.tmpFile)
		if err != nil {
			x.tmpFile = nil // prevent re-closure
			err = x.handleFileError(err)
			if err == nil {
				continue
			}
			return err
		}

		return renameFile(x.tmpPath, x.targetPath)
	}
}

func (w *genericWriter) initWriteData(_ oid.ID, filePath string, _ uint64) (io.WriteCloser, func(), error) {
	stream := newGenericFileWriteStream(w, filePath)
	return stream, stream.abort, nil
}

func (w *genericWriter) writeData(_ oid.ID, p string, data []byte) error {
	// Here is a situation:
	// Feb 09 13:10:37 buky neofs-node[32445]: 2023-02-09T13:10:37.161Z        info        log/log.go:13        local object storage operation        {"shard_id": "SkT8BfjouW6t93oLuzQ79s", "address": "7NxFz4SruSi8TqXacr2Ae22nekMhgYk1sfkddJo9PpWk/5enyUJGCyU1sfrURDnHEjZFdbGqANVhayYGfdSqtA6wA", "op": "PUT", "type": "fstree", "storage_id": ""}
	// Feb 09 13:10:37 buky neofs-node[32445]: 2023-02-09T13:10:37.183Z        info        log/log.go:13        local object storage operation        {"shard_id": "SkT8BfjouW6t93oLuzQ79s", "address": "7NxFz4SruSi8TqXacr2Ae22nekMhgYk1sfkddJo9PpWk/5enyUJGCyU1sfrURDnHEjZFdbGqANVhayYGfdSqtA6wA", "op": "metabase PUT"}
	// Feb 09 13:10:37 buky neofs-node[32445]: 2023-02-09T13:10:37.862Z        debug        policer/check.go:231        shortage of object copies detected        {"component": "Object Policer", "object": "7NxFz4SruSi8TqXacr2Ae22nekMhgYk1sfkddJo9PpWk/5enyUJGCyU1sfrURDnHEjZFdbGqANVhayYGfdSqtA6wA", "shortage": 1}
	// Feb 09 13:10:37 buky neofs-node[32445]: 2023-02-09T13:10:37.862Z        debug        shard/get.go:124        object is missing in write-cache        {"shard_id": "SkT8BfjouW6t93oLuzQ79s", "addr": "7NxFz4SruSi8TqXacr2Ae22nekMhgYk1sfkddJo9PpWk/5enyUJGCyU1sfrURDnHEjZFdbGqANVhayYGfdSqtA6wA", "skip_meta": false}
	//
	// 1. We put an object on node 1.
	// 2. Relentless policer sees that it has only 1 copy and tries to PUT it to node 2.
	// 3. PUT operation started by client at (1) also puts an object here.
	// 4. Now we have concurrent writes and one of `Rename` calls will return `no such file` error.
	//    Even more than that, concurrent writes can corrupt data.
	//
	// So here is a solution:
	// 1. Write a file to 'name + 1'.
	// 2. If it exists, retry with temporary name being 'name + 2'.
	// 3. Set some reasonable number of attempts.
	//
	// It is a bit kludgey, but I am unusually proud about having found this out after
	// hours of research on linux kernel, dirsync mount option and ext4 FS, turned out
	// to be so hecking simple.
	// In a very rare situation we can have multiple partially written copies on disk,
	// this will be fixed in another issue (we should remove garbage on start).
	for i := range genericFileWriteRetryCount {
		tmpPath := p + "#" + strconv.FormatUint(uint64(i), 10)
		err := w.writeAndRename(tmpPath, p, data)
		if !errors.Is(err, syscall.EEXIST) || i == genericFileWriteRetryCount-1 {
			return err
		}
	}

	// unreachable, but precaution never hurts, especially 1 day before release.
	return fmt.Errorf("couldn't write file after %d retries", genericFileWriteRetryCount)
}

// writeAndRename opens tmpPath exclusively, writes data to it and renames it to p.
func (w *genericWriter) writeAndRename(tmpPath, p string, data []byte) error {
	err := w.writeFile(tmpPath, data)
	if err != nil {
		err = handleFileError(tmpPath, err)
		if errors.Is(err, common.ErrNoSpace) {
			_ = os.RemoveAll(tmpPath)
		}
		return err
	}

	return renameFile(tmpPath, p)
}

// writeFile writes data to a file with path p.
// The code is copied from `os.WriteFile` with minor corrections for flags.
func (w *genericWriter) writeFile(p string, data []byte) error {
	f, err := w.openFile(p)
	if err != nil {
		return err
	}
	_, err = writeToFile(f, data)
	if err != nil {
		return err
	}
	return closeFile(f)
}

func (w *genericWriter) openFile(name string) (*os.File, error) {
	f, err := os.OpenFile(name, w.flags, w.perm)
	if err != nil {
		return nil, fmt.Errorf("open file with flags %d: %w", w.flags, err)
	}
	return f, nil
}

func writeToFile(f *os.File, data []byte) (int, error) {
	n, err := f.Write(data)
	if err != nil {
		_ = f.Close()
		return n, fmt.Errorf("write data to the file: %w", err)
	}
	return n, nil
}

func closeFile(f *os.File) error {
	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	return nil
}

func renameFile(from string, to string) error {
	if err := os.Rename(from, to); err != nil {
		return fmt.Errorf("rename file %q->%q: %w", from, to, err)
	}
	return nil
}

func newFilePathForTry(targetPath string, tryIdx int) string {
	return targetPath + "#" + strconv.FormatUint(uint64(tryIdx), 10)
}

func handleFileError(tmpPath string, err error) error {
	if pe, ok := errors.AsType[*fs.PathError](err); ok {
		switch {
		case errors.Is(pe.Err, syscall.ENOSPC):
			err = common.ErrNoSpace
		case errors.Is(pe.Err, syscall.EEXIST):
			return syscall.EEXIST
		}
	}

	return fmt.Errorf("write data into file %q: %w", tmpPath, err)
}
