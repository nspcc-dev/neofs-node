package fstree

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// FSTree represents an object storage as a filesystem tree.
type FSTree struct {
	Info

	Depth      int
	DirNameLen int
}

// Info groups the information about file storage.
type Info struct {
	// Permission bits of the root directory.
	Permissions fs.FileMode

	// Full path to the root directory.
	RootPath string
}

const (
	// DirNameLen is how many bytes is used to group keys into directories.
	DirNameLen = 1 // in bytes
	// MaxDepth is maximum depth of nested directories.
	MaxDepth = (sha256.Size - 1) / DirNameLen
)

// ErrFileNotFound is returned when file is missing.
var ErrFileNotFound = errors.New("file not found")

func stringifyAddress(addr oid.Address) string {
	return addr.Object().EncodeToString() + "." + addr.Container().EncodeToString()
}

func addressFromString(s string) (*oid.Address, error) {
	ss := strings.SplitN(s, ".", 2)
	if len(ss) != 2 {
		return nil, errors.New("invalid address")
	}

	var obj oid.ID
	if err := obj.DecodeString(ss[0]); err != nil {
		return nil, err
	}

	var cnr cid.ID
	if err := cnr.DecodeString(ss[1]); err != nil {
		return nil, err
	}

	var addr oid.Address
	addr.SetObject(obj)
	addr.SetContainer(cnr)

	return &addr, nil
}

// Iterate iterates over all stored objects.
func (t *FSTree) Iterate(prm common.IteratePrm) (common.IterateRes, error) {
	return common.IterateRes{}, t.iterate(0, []string{t.RootPath}, prm)
}

func (t *FSTree) iterate(depth int, curPath []string, prm common.IteratePrm) error {
	curName := strings.Join(curPath[1:], "")
	des, err := os.ReadDir(filepath.Join(curPath...))
	if err != nil {
		if prm.IgnoreErrors {
			return nil
		}
		return err
	}

	isLast := depth >= t.Depth
	l := len(curPath)
	curPath = append(curPath, "")

	for i := range des {
		curPath[l] = des[i].Name()

		if !isLast && des[i].IsDir() {
			err := t.iterate(depth+1, curPath, prm)
			if err != nil {
				// Must be error from handler in case errors are ignored.
				// Need to report.
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

		if prm.LazyHandler != nil {
			err = prm.LazyHandler(*addr, func() ([]byte, error) {
				return os.ReadFile(filepath.Join(curPath...))
			})
		} else {
			var data []byte
			data, err = os.ReadFile(filepath.Join(curPath...))
			if err != nil {
				if prm.IgnoreErrors {
					if prm.ErrorHandler != nil {
						return prm.ErrorHandler(*addr, err)
					}
					continue
				}
				return err
			}

			err = prm.Handler(common.IterationElement{
				Address:    *addr,
				ObjectData: data,
				StorageID:  []byte{},
			})
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (t *FSTree) treePath(addr oid.Address) string {
	sAddr := stringifyAddress(addr)

	dirs := make([]string, 0, t.Depth+1+1) // 1 for root, 1 for file
	dirs = append(dirs, t.RootPath)

	for i := 0; i < t.Depth; i++ {
		dirs = append(dirs, sAddr[:t.DirNameLen])
		sAddr = sAddr[t.DirNameLen:]
	}

	dirs = append(dirs, sAddr)

	return filepath.Join(dirs...)
}

// Delete removes the object with the specified address from the storage.
func (t *FSTree) Delete(addr oid.Address) error {
	p, err := t.Exists(addr)
	if err != nil {
		return err
	}

	return os.Remove(p)
}

// Exists returns the path to the file with object contents if it exists in the storage
// and an error otherwise.
func (t *FSTree) Exists(addr oid.Address) (string, error) {
	p := t.treePath(addr)

	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		err = ErrFileNotFound
	}

	return p, err
}

// Put puts an object in the storage.
func (t *FSTree) Put(prm common.PutPrm) (common.PutRes, error) {
	p := t.treePath(prm.Address)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return common.PutRes{}, err
	}

	return common.PutRes{}, os.WriteFile(p, prm.RawData, t.Permissions)
}

// PutStream puts executes handler on a file opened for write.
func (t *FSTree) PutStream(addr oid.Address, handler func(*os.File) error) error {
	p := t.treePath(addr)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return err
	}

	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, t.Permissions)
	if err != nil {
		return err
	}
	defer f.Close()

	return handler(f)
}

// Get returns an object from the storage by address.
func (t *FSTree) Get(prm common.GetPrm) ([]byte, error) {
	p := t.treePath(prm.Address)

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, ErrFileNotFound
	}

	return os.ReadFile(p)
}

// NumberOfObjects walks the file tree rooted at FSTree's root
// and returns number of stored objects.
func (t *FSTree) NumberOfObjects() (uint64, error) {
	var counter uint64

	// it is simpler to just consider every file
	// that is not directory as an object
	err := filepath.WalkDir(t.RootPath,
		func(_ string, d fs.DirEntry, _ error) error {
			if !d.IsDir() {
				counter++
			}

			return nil
		},
	)
	if err != nil {
		return 0, fmt.Errorf("could not walk through %s directory: %w", t.RootPath, err)
	}

	return counter, nil
}
