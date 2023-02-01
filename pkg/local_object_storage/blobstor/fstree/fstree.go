package fstree

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// FSTree represents an object storage as a filesystem tree.
type FSTree struct {
	Info

	*compression.Config
	Depth      uint64
	DirNameLen int

	noSync   bool
	readOnly bool
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

var _ common.Storage = (*FSTree)(nil)

func New(opts ...Option) *FSTree {
	f := &FSTree{
		Info: Info{
			Permissions: 0700,
			RootPath:    "./",
		},
		Config:     nil,
		Depth:      4,
		DirNameLen: DirNameLen,
	}
	for i := range opts {
		opts[i](f)
	}

	return f
}

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

func (t *FSTree) iterate(depth uint64, curPath []string, prm common.IteratePrm) error {
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
			if err == nil {
				data, err = t.Decompress(data)
			}
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

	for i := 0; uint64(i) < t.Depth; i++ {
		dirs = append(dirs, sAddr[:t.DirNameLen])
		sAddr = sAddr[t.DirNameLen:]
	}

	dirs = append(dirs, sAddr)

	return filepath.Join(dirs...)
}

// Delete removes the object with the specified address from the storage.
func (t *FSTree) Delete(prm common.DeletePrm) (common.DeleteRes, error) {
	if t.readOnly {
		return common.DeleteRes{}, common.ErrReadOnly
	}

	p, err := t.getPath(prm.Address)
	if err != nil {
		if os.IsNotExist(err) {
			err = logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return common.DeleteRes{}, err
	}

	err = os.Remove(p)
	if err != nil && os.IsNotExist(err) {
		err = logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	return common.DeleteRes{}, err
}

// Exists returns the path to the file with object contents if it exists in the storage
// and an error otherwise.
func (t *FSTree) Exists(prm common.ExistsPrm) (common.ExistsRes, error) {
	_, err := t.getPath(prm.Address)
	found := err == nil
	if os.IsNotExist(err) {
		err = nil
	}
	return common.ExistsRes{Exists: found}, err
}

func (t *FSTree) getPath(addr oid.Address) (string, error) {
	p := t.treePath(addr)

	_, err := os.Stat(p)
	return p, err
}

// Put puts an object in the storage.
func (t *FSTree) Put(prm common.PutPrm) (common.PutRes, error) {
	if t.readOnly {
		return common.PutRes{}, common.ErrReadOnly
	}

	p := t.treePath(prm.Address)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return common.PutRes{}, err
	}
	if !prm.DontCompress {
		prm.RawData = t.Compress(prm.RawData)
	}

	tmpPath := p + "#"
	err := t.writeFile(tmpPath, prm.RawData)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) && pe.Err == syscall.ENOSPC {
			err = common.ErrNoSpace
			_ = os.RemoveAll(tmpPath)
		}
	} else {
		err = os.Rename(tmpPath, p)
	}

	return common.PutRes{StorageID: []byte{}}, err
}

func (t *FSTree) writeFlags() int {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if t.noSync {
		return flags
	}
	return flags | os.O_SYNC
}

// writeFile writes data to a file with path p.
// The code is copied from `os.WriteFile` with minor corrections for flags.
func (t *FSTree) writeFile(p string, data []byte) error {
	f, err := os.OpenFile(p, t.writeFlags(), t.Permissions)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

// PutStream puts executes handler on a file opened for write.
func (t *FSTree) PutStream(addr oid.Address, handler func(*os.File) error) error {
	if t.readOnly {
		return common.ErrReadOnly
	}

	p := t.treePath(addr)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return err
	}

	f, err := os.OpenFile(p, t.writeFlags(), t.Permissions)
	if err != nil {
		return err
	}
	defer f.Close()

	return handler(f)
}

// Get returns an object from the storage by address.
func (t *FSTree) Get(prm common.GetPrm) (common.GetRes, error) {
	p := t.treePath(prm.Address)

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return common.GetRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	data, err := os.ReadFile(p)
	if err != nil {
		return common.GetRes{}, err
	}

	data, err = t.Decompress(data)
	if err != nil {
		return common.GetRes{}, err
	}

	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return common.GetRes{}, err
	}

	return common.GetRes{Object: obj, RawData: data}, err
}

// GetRange implements common.Storage.
func (t *FSTree) GetRange(prm common.GetRangePrm) (common.GetRangeRes, error) {
	res, err := t.Get(common.GetPrm{Address: prm.Address})
	if err != nil {
		return common.GetRangeRes{}, err
	}

	payload := res.Object.Payload()
	from := prm.Range.GetOffset()
	to := from + prm.Range.GetLength()

	if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
		return common.GetRangeRes{}, logicerr.Wrap(apistatus.ObjectOutOfRange{})
	}

	return common.GetRangeRes{
		Data: payload[from:to],
	}, nil
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

// Type is fstree storage type used in logs and configuration.
const Type = "fstree"

// Type implements common.Storage.
func (*FSTree) Type() string {
	return Type
}

// Path implements common.Storage.
func (t *FSTree) Path() string {
	return t.RootPath
}

// SetCompressor implements common.Storage.
func (t *FSTree) SetCompressor(cc *compression.Config) {
	t.Config = cc
}

// SetReportErrorFunc implements common.Storage.
func (t *FSTree) SetReportErrorFunc(f func(string, error)) {
	// Do nothing, FSTree can encounter only one error which is returned.
}
