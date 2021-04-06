package fstree

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"

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
	h := sha256.Sum256([]byte(addr.String()))

	return hex.EncodeToString(h[:])
}

// Iterate iterates over all stored objects.
func (t *FSTree) Iterate(f func(addr *objectSDK.Address, data []byte) error) error {
	return t.iterate(0, []string{t.RootPath}, f)
}

func (t *FSTree) iterate(depth int, curPath []string, f func(*objectSDK.Address, []byte) error) error {
	curName := strings.Join(curPath[1:], "")
	des, err := ioutil.ReadDir(curName)
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

		addr := objectSDK.NewAddress()
		err := addr.Parse(curName + des[i].Name())
		if err != nil {
			continue
		}

		curPath = append(curPath, des[i].Name())
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
