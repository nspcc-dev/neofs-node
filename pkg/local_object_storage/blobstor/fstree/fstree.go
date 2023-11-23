package fstree

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

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
	writeData  func(string, []byte) error

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
	f.writeData = newGenericWriteData(f.Permissions, f.noSync)

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
		return nil, fmt.Errorf("decode object ID from string %q: %w", ss[0], err)
	}

	var cnr cid.ID
	if err := cnr.DecodeString(ss[1]); err != nil {
		return nil, fmt.Errorf("decode container ID from string %q: %w", ss[1], err)
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
	dir := filepath.Join(curPath...)
	des, err := os.ReadDir(dir)
	if err != nil {
		if prm.IgnoreErrors {
			return nil
		}
		return fmt.Errorf("read dir %q: %w", dir, err)
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
				data, err := os.ReadFile(filepath.Join(curPath...))
				if err != nil && errors.Is(err, fs.ErrNotExist) {
					return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
				}

				return data, err
			})
		} else {
			var data []byte
			p := filepath.Join(curPath...)
			data, err = os.ReadFile(p)
			if err != nil && errors.Is(err, fs.ErrNotExist) {
				continue
			}
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
				return fmt.Errorf("read file %q: %w", p, err)
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
		if errors.Is(err, fs.ErrNotExist) {
			err = logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		return common.DeleteRes{}, err
	}

	err = os.Remove(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return common.DeleteRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
		}

		return common.DeleteRes{}, fmt.Errorf("remove file %q: %w", p, err)
	}

	return common.DeleteRes{}, nil
}

// Exists returns the path to the file with object contents if it exists in the storage
// and an error otherwise.
func (t *FSTree) Exists(prm common.ExistsPrm) (common.ExistsRes, error) {
	_, err := t.getPath(prm.Address)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return common.ExistsRes{Exists: false}, nil
		}

		return common.ExistsRes{}, err
	}

	return common.ExistsRes{Exists: true}, nil
}

// checks whether file for the given object address exists and returns path to
// the file if so. Returns [fs.ErrNotExist] if file is missing.
func (t *FSTree) getPath(addr oid.Address) (string, error) {
	p := t.treePath(addr)

	_, err := os.Stat(p)
	if err != nil {
		return "", fmt.Errorf("get filesystem path for object by address: get file stat %q: %w", p, err)
	}

	return p, nil
}

// Put puts an object in the storage.
func (t *FSTree) Put(prm common.PutPrm) (common.PutRes, error) {
	if t.readOnly {
		return common.PutRes{}, common.ErrReadOnly
	}

	p := t.treePath(prm.Address)

	if err := util.MkdirAllX(filepath.Dir(p), t.Permissions); err != nil {
		return common.PutRes{}, fmt.Errorf("mkdirall for %q: %w", p, err)
	}
	if !prm.DontCompress {
		prm.RawData = t.Compress(prm.RawData)
	}
	err := t.writeData(p, prm.RawData)
	if err != nil {
		return common.PutRes{}, fmt.Errorf("write object data into file %q: %w", p, err)
	}
	return common.PutRes{StorageID: []byte{}}, nil
}

// Get returns an object from the storage by address.
func (t *FSTree) Get(prm common.GetPrm) (common.GetRes, error) {
	p := t.treePath(prm.Address)

	if _, err := os.Stat(p); errors.Is(err, fs.ErrNotExist) {
		return common.GetRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	data, err := os.ReadFile(p)
	if err != nil {
		return common.GetRes{}, fmt.Errorf("read file %q: %w", p, err)
	}

	data, err = t.Decompress(data)
	if err != nil {
		return common.GetRes{}, fmt.Errorf("decompress file data %q: %w", p, err)
	}

	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return common.GetRes{}, fmt.Errorf("decode object from file %q: %w", p, err)
	}

	return common.GetRes{Object: obj, RawData: data}, nil
}

// OpenObjectStream looks up for referenced object in the FSTree and, if the
// object exists, opens and returns stream with binary-encoded object. Returns
// [fs.ErrNotExist] if object was not found. Resulting stream must be finally
// closed.
func (t *FSTree) OpenObjectStream(objAddr oid.Address) (io.ReadSeekCloser, error) {
	p := t.treePath(objAddr)

	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open object file %q: %w", p, err)
	}

	return f, nil
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
		return 0, fmt.Errorf("could not walk through %q directory: %w", t.RootPath, err)
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
func (t *FSTree) SetReportErrorFunc(_ func(string, error)) {
	// Do nothing, FSTree can encounter only one error which is returned.
}
