package fstree

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// RewriteCompressedStats groups [FSTree.RewriteCompressed] counters.
type RewriteCompressedStats struct {
	Scanned         uint64
	Compressed      uint64
	Rewritten       uint64
	Skipped         uint64
	Failed          uint64
	CompressedBytes uint64
	PlainBytes      uint64
}

type compressedEntry struct {
	data []byte
}

var (
	errRewriteCompressedObjectGone     = errors.New("object disappeared during rewrite")
	errRewriteCompressedObjectReplaced = errors.New("object changed during rewrite")
)

// RewriteCompressed scans FSTree object files, decompresses zstd-compressed
// object bytes and stores canonical uncompressed object bytes back.
//
// On Linux, the storage node may write to the FSTree while the migration is
// running. Other platforms must stop the storage node before migration.
// Compressed objects in combined physical files are rewritten as individual
// object files.
func (t *FSTree) RewriteCompressed() (RewriteCompressedStats, error) {
	if t.readOnly {
		return RewriteCompressedStats{}, common.ErrReadOnly
	}
	if err := checkRewriteCompressedOnlineSupport(t.RootPath); err != nil {
		return RewriteCompressedStats{}, err
	}

	var st RewriteCompressedStats
	err := t.walkObjectFiles(func(path string, addr oid.Address) error {
		return t.rewriteCompressedObject(path, addr, &st)
	})
	if err != nil {
		return st, err
	}
	return st, nil
}

func (t *FSTree) rewriteCompressedObject(path string, addr oid.Address, st *RewriteCompressedStats) error {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		st.Failed++
		return fmt.Errorf("open object file %q: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		st.Failed++
		return fmt.Errorf("stat object file %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	owner, err := newFileOwner(path, info)
	if err != nil {
		st.Failed++
		return err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		st.Failed++
		return fmt.Errorf("read object file %q: %w", path, err)
	}

	entry, _, err := compressedEntryForAddress(data, addr.Object())
	if err != nil {
		st.Failed++
		return fmt.Errorf("parse object file %q: %w", path, err)
	}

	st.Scanned++
	if !isCompressed(entry.data) {
		st.Skipped++
		return nil
	}

	st.Compressed++
	st.CompressedBytes += uint64(len(entry.data))
	plain, err := decompress(entry.data)
	if err != nil {
		st.Failed++
		return fmt.Errorf("decompress object %s: %w", addr, err)
	}
	st.PlainBytes += uint64(len(plain))

	err = rewriteCompressedObjectFile(path, info, owner, plain, t.noSync)
	if errors.Is(err, errRewriteCompressedObjectGone) || errors.Is(err, errRewriteCompressedObjectReplaced) {
		return nil
	}
	if err != nil {
		st.Failed++
		return err
	}
	st.Rewritten++
	return nil
}

func compressedEntryForAddress(data []byte, id oid.ID) (compressedEntry, bool, error) {
	if entryID, _ := parseCombinedPrefix(data); entryID == nil {
		return compressedEntry{data: data}, false, nil
	}

	for len(data) > 0 {
		entryID, ln := parseCombinedPrefix(data)
		if entryID == nil {
			return compressedEntry{}, true, errors.New("invalid combined entry prefix")
		}
		if uint64(ln) > uint64(len(data)-combinedDataOff) {
			return compressedEntry{}, true, errors.New("invalid combined entry length")
		}
		var objID oid.ID
		copy(objID[:], entryID)
		if objID == id {
			return compressedEntry{data: data[combinedDataOff : combinedDataOff+int(ln)]}, true, nil
		}
		data = data[combinedDataOff+int(ln):]
	}
	return compressedEntry{}, true, fmt.Errorf("combined file does not contain object %s", id)
}

func (t *FSTree) walkObjectFiles(f func(string, oid.Address) error) error {
	var walk func(uint64, []string) error
	walk = func(depth uint64, curPath []string) error {
		curName := strings.Join(curPath[1:], "")
		dir := filepath.Join(curPath...)
		des, err := os.ReadDir(dir)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read dir %q: %w", dir, err)
		}

		isLast := depth >= t.Depth
		l := len(curPath)
		curPath = append(curPath, "")
		for i := range des {
			curPath[l] = des[i].Name()
			if !isLast && des[i].IsDir() {
				if err = walk(depth+1, curPath); err != nil {
					return err
				}
			}
			if depth != t.Depth {
				continue
			}

			addr, err := addressFromString(curName + des[i].Name())
			if err != nil {
				continue
			}
			p := filepath.Join(curPath...)
			info, err := des[i].Info()
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return fmt.Errorf("stat object file %q: %w", p, err)
			}
			if !info.Mode().IsRegular() {
				continue
			}
			if err = f(p, *addr); err != nil {
				return fmt.Errorf("handle object file %q: %w", p, err)
			}
		}
		return nil
	}
	return walk(0, []string{t.RootPath})
}
