package fstree

import (
	"crypto/sha256"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
)

// FSTree represents object storage as filesystem tree.
type FSTree struct {
	Info

	Depth      int
	DirNameLen int
}

// Info groups the information about file storage.
type Info struct {
	// Permission bits of the root directory.
	Permissions os.FileMode

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

func stringifyAddress(addr *objectSDK.Address) string {
	return addr.ObjectID().String() + "." + addr.ContainerID().String()
}

func addressFromString(s string) (*objectSDK.Address, error) {
	ss := strings.SplitN(s, ".", 2)
	if len(ss) != 2 {
		return nil, errors.New("invalid address")
	}

	oid := objectSDK.NewID()
	if err := oid.Parse(ss[0]); err != nil {
		return nil, err
	}

	id := cid.New()
	if err := id.Parse(ss[1]); err != nil {
		return nil, err
	}

	addr := objectSDK.NewAddress()
	addr.SetObjectID(oid)
	addr.SetContainerID(id)

	return addr, nil
}

// Iterate iterates over all stored objects.
func (t *FSTree) Iterate(f func(addr *objectSDK.Address, data []byte) error) error {
	return t.iterate(0, []string{t.RootPath}, f)
}

func (t *FSTree) iterate(depth int, curPath []string, f func(*objectSDK.Address, []byte) error) error {
	curName := strings.Join(curPath[1:], "")
	des, err := ioutil.ReadDir(path.Join(curPath...))
	if err != nil {
		return err
	}

	isLast := depth >= t.Depth
	l := len(curPath)
	curPath = append(curPath, "")

	for i := range des {
		curPath[l] = des[i].Name()

		if !isLast && des[i].IsDir() {
			err := t.iterate(depth+1, curPath, f)
			if err != nil {
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

		data, err := ioutil.ReadFile(path.Join(curPath...))
		if err != nil {
			return err
		}

		if err := f(addr, data); err != nil {
			return err
		}
	}

	return nil
}

func (t *FSTree) treePath(addr *objectSDK.Address) string {
	sAddr := stringifyAddress(addr)

	dirs := make([]string, 0, t.Depth+1+1) // 1 for root, 1 for file
	dirs = append(dirs, t.RootPath)

	for i := 0; i < t.Depth; i++ {
		dirs = append(dirs, sAddr[:t.DirNameLen])
		sAddr = sAddr[t.DirNameLen:]
	}

	dirs = append(dirs, sAddr)

	return path.Join(dirs...)
}

// Delete removes object with the specified address from storage.
func (t *FSTree) Delete(addr *objectSDK.Address) error {
	p, err := t.Exists(addr)
	if err != nil {
		return err
	}

	return os.Remove(p)
}

// Exists returns path to file with object contents if it exists in storage
// and an error otherwise.
func (t *FSTree) Exists(addr *objectSDK.Address) (string, error) {
	p := t.treePath(addr)

	_, err := os.Stat(p)
	if os.IsNotExist(err) {
		err = ErrFileNotFound
	}

	return p, err
}

// Put puts object in storage.
func (t *FSTree) Put(addr *objectSDK.Address, data []byte) error {
	p := t.treePath(addr)

	if err := os.MkdirAll(path.Dir(p), t.Permissions); err != nil {
		return err
	}

	return ioutil.WriteFile(p, data, t.Permissions)
}

// Get returns object from storage by address.
func (t *FSTree) Get(addr *objectSDK.Address) ([]byte, error) {
	p := t.treePath(addr)

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil, ErrFileNotFound
	}

	return ioutil.ReadFile(p)
}
