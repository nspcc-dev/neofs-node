package peapod

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type batch struct {
	initErr error

	tx *bbolt.Tx

	nonIdle bool

	commitErr   error
	chCommitted chan struct{}

	bktRootMtx sync.Mutex
	bktRoot    *bbolt.Bucket
}

// Peapod provides storage for relatively small NeoFS binary object (peas).
// Peapod is a single low-level key/value database optimized to work with big
// number of stored units.
//
// Deprecated: Use fstree.FSTree instead.
type Peapod struct {
	path string
	perm fs.FileMode

	flushInterval time.Duration

	compress *compression.Config
	log      *zap.Logger

	readOnly bool

	bolt *bbolt.DB

	currentBatchMtx sync.RWMutex
	currentBatch    *batch

	chClose     chan struct{}
	chFlushDone chan struct{}
}

var rootBucket = []byte("root")

// returned when BoltDB rootBucket is inaccessible within particular transaction.
var errMissingRootBucket = errors.New("missing root bucket")

// New creates new Peapod instance to be located at the given path with
// specified permissions.
//
// Specified flush interval MUST be positive (see Init).
//
// Note that resulting Peapod is NOT ready-to-go:
//   - configure compression first (SetCompressor method)
//   - then open the instance (Open method). Opened Peapod must be finally closed
//   - initialize internal database structure (Init method). May be skipped for read-only usage
//
// Any other usage is unsafe and may lead to panic.
func New(path string, perm fs.FileMode, flushInterval time.Duration) *Peapod {
	if flushInterval <= 0 {
		panic(fmt.Sprintf("non-positive flush interval %v", flushInterval))
	}
	return &Peapod{
		path: path,
		perm: perm,

		flushInterval: flushInterval,
	}
}

func (x *Peapod) flushLoop() {
	defer close(x.chFlushDone)

	t := time.NewTimer(x.flushInterval)
	defer t.Stop()

	for {
		select {
		case <-x.chClose:
			// commit current transaction to prevent bbolt.DB.Close blocking
			x.flushCurrentBatch(false)
			return
		case <-t.C:
			st := time.Now()

			x.flushCurrentBatch(true)

			interval := x.flushInterval - time.Since(st)
			if interval <= 0 {
				interval = time.Microsecond
			}

			t.Reset(interval)
		}
	}
}

func (x *Peapod) flushCurrentBatch(beginNew bool) {
	x.currentBatchMtx.Lock()

	if !x.currentBatch.nonIdle {
		if !beginNew && x.currentBatch.tx != nil {
			_ = x.currentBatch.tx.Commit()
		}
		x.currentBatchMtx.Unlock()
		return
	}

	err := x.currentBatch.tx.Commit()
	if err != nil {
		err = fmt.Errorf("commit BoltDB batch transaction: %w", err)
	}

	x.currentBatch.commitErr = err
	close(x.currentBatch.chCommitted)

	if beginNew {
		x.beginNewBatch()
	}

	x.currentBatchMtx.Unlock()
}

func (x *Peapod) beginNewBatch() {
	x.currentBatch = new(batch)

	x.currentBatch.tx, x.currentBatch.initErr = x.bolt.Begin(true)
	if x.currentBatch.initErr != nil {
		x.currentBatch.initErr = fmt.Errorf("begin new BoltDB writable transaction: %w", x.currentBatch.initErr)
		return
	}

	x.currentBatch.bktRoot = x.currentBatch.tx.Bucket(rootBucket)
	if x.currentBatch.bktRoot == nil {
		x.currentBatch.initErr = errMissingRootBucket
		return
	}

	x.currentBatch.chCommitted = make(chan struct{})
}

const objectAddressKeySize = 2 * sha256.Size

func keyForObject(addr oid.Address) []byte {
	b := make([]byte, objectAddressKeySize)
	cnr := addr.Container()
	obj := addr.Object()
	n := copy(b, cnr[:])
	copy(b[n:], obj[:])
	return b
}

func decodeKeyForObject(addr *oid.Address, key []byte) error {
	if len(key) != objectAddressKeySize {
		return fmt.Errorf("invalid object address key size: %d instead of %d", len(key), objectAddressKeySize)
	}

	var cnr cid.ID
	var obj oid.ID

	err := cnr.Decode(key[:sha256.Size])
	if err != nil {
		return fmt.Errorf("decode container ID: %w", err)
	}

	err = obj.Decode(key[sha256.Size:])
	if err != nil {
		return fmt.Errorf("decode object ID: %w", err)
	}

	addr.SetContainer(cnr)
	addr.SetObject(obj)

	return nil
}

// Open opens underlying database in the specified mode.
func (x *Peapod) Open(readOnly bool) error {
	err := util.MkdirAllX(filepath.Dir(x.path), x.perm)
	if err != nil {
		return fmt.Errorf("create directory '%s' for database: %w", x.path, err)
	}

	x.bolt, err = bbolt.Open(x.path, x.perm, &bbolt.Options{
		ReadOnly: readOnly,
		Timeout:  time.Second, // to handle flock
	})
	if err != nil {
		return fmt.Errorf("open BoltDB instance: %w", err)
	}

	x.readOnly = readOnly

	return nil
}

// Init initializes internal structure of the underlying database and runs
// flushing routine. The routine writes data batches into disk once per time
// interval configured in New.
func (x *Peapod) Init() error {
	if x.readOnly {
		err := x.bolt.View(func(tx *bbolt.Tx) error {
			if tx.Bucket(rootBucket) == nil {
				return errMissingRootBucket
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("check root bucket presence in BoltDB instance: %w", err)
		}
		return nil
	}

	err := x.bolt.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(rootBucket)
		return err
	})
	if err != nil {
		return fmt.Errorf("create root bucket in BoltDB instance: %w", err)
	}

	x.chClose = make(chan struct{})
	x.chFlushDone = make(chan struct{})

	x.beginNewBatch()

	go x.flushLoop()

	return nil
}

// Close syncs data and closes the database.
func (x *Peapod) Close() error {
	if !x.readOnly && x.chClose != nil {
		close(x.chClose)
		<-x.chFlushDone
		x.chClose = nil
	}
	return x.bolt.Close()
}

// Type is peapod storage type used in logs and configuration.
const Type = "peapod"

func (x *Peapod) Type() string {
	return Type
}

func (x *Peapod) Path() string {
	return x.path
}

func (x *Peapod) SetCompressor(cc *compression.Config) {
	x.compress = cc
}

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (x *Peapod) SetLogger(l *zap.Logger) {
	x.log = l.With(zap.String("substorage", Type))
}

// Get reads data from the underlying database by the given object address.
// Returns apistatus.ErrObjectNotFound if object is missing in the Peapod.
func (x *Peapod) Get(addr oid.Address) (*objectSDK.Object, error) {
	var data []byte

	err := x.bolt.View(func(tx *bbolt.Tx) error {
		bktRoot := tx.Bucket(rootBucket)
		if bktRoot == nil {
			return errMissingRootBucket
		}

		val := bktRoot.Get(keyForObject(addr))
		if val == nil {
			return apistatus.ErrObjectNotFound
		}

		data = bytes.Clone(val)

		return nil
	})
	if err != nil {
		if errors.Is(err, apistatus.ErrObjectNotFound) {
			return nil, logicerr.Wrap(err)
		}
		return nil, fmt.Errorf("exec read-only BoltDB transaction: %w", err)
	}

	// copy-paste from FSTree
	data, err = x.compress.Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("decompress data: %w", err)
	}

	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("decode object from binary: %w", err)
	}

	return obj, err
}

// GetBytes reads object from the Peapod by address into memory buffer in a
// canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
func (x *Peapod) GetBytes(addr oid.Address) ([]byte, error) {
	var b []byte

	err := x.bolt.View(func(tx *bbolt.Tx) error {
		bktRoot := tx.Bucket(rootBucket)
		if bktRoot == nil {
			return errMissingRootBucket
		}

		val := bktRoot.Get(keyForObject(addr))
		if val == nil {
			return apistatus.ErrObjectNotFound
		}

		b = bytes.Clone(val)

		return nil
	})
	if err != nil {
		if errors.Is(err, apistatus.ErrObjectNotFound) {
			return nil, logicerr.Wrap(err)
		}
		return nil, fmt.Errorf("exec read-only BoltDB transaction: %w", err)
	}

	// copy-paste from FSTree
	if !x.compress.IsCompressed(b) {
		return b, nil
	}

	dec, err := x.compress.DecompressForce(b)
	if err != nil {
		return nil, fmt.Errorf("decompress object BoltDB data: %w", err)
	}

	return dec, nil
}

// GetRange works like Get but reads specific payload range.
func (x *Peapod) GetRange(addr oid.Address, from uint64, length uint64) ([]byte, error) {
	// copy-paste from FSTree
	obj, err := x.Get(addr)
	if err != nil {
		return nil, err
	}

	payload := obj.Payload()
	to := from + length

	if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
		return nil, logicerr.Wrap(apistatus.ObjectOutOfRange{})
	}

	return payload[from:to], nil
}

// Exists checks presence of the object in the underlying database by the given
// address.
func (x *Peapod) Exists(addr oid.Address) (bool, error) {
	var res bool

	err := x.bolt.View(func(tx *bbolt.Tx) error {
		bktRoot := tx.Bucket(rootBucket)
		if bktRoot == nil {
			return errMissingRootBucket
		}

		res = bktRoot.Get(keyForObject(addr)) != nil

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("exec read-only BoltDB transaction: %w", err)
	}

	return res, nil
}

var storageID = []byte("peapod")

// Put saves given data in the underlying database by specified object address.
// The data can be anything, but in practice a binary NeoFS object is expected.
// Operation is executed within provided context: if the context is done, Put
// returns its error (in this case data may be saved).
//
// Put returns common.ErrReadOnly if Peadpod is read-only.
func (x *Peapod) Put(prm common.PutPrm) (common.PutRes, error) {
	if !prm.DontCompress {
		prm.RawData = x.compress.Compress(prm.RawData)
	}

	// Track https://github.com/nspcc-dev/neofs-node/issues/2480
	err := x.batch(context.TODO(), func(bktRoot *bbolt.Bucket) error {
		return bktRoot.Put(keyForObject(prm.Address), prm.RawData)
	})

	return common.PutRes{
		StorageID: storageID,
	}, err
}

// Delete removes data associated with the given object address from the
// underlying database. Delete returns apistatus.ErrObjectNotFound if object is
// missing.
//
// Put returns common.ErrReadOnly if Peadpod is read-only.
func (x *Peapod) Delete(addr oid.Address) error {
	// Track https://github.com/nspcc-dev/neofs-node/issues/2480
	err := x.batch(context.TODO(), func(bktRoot *bbolt.Bucket) error {
		key := keyForObject(addr)
		if bktRoot.Get(key) == nil {
			return apistatus.ErrObjectNotFound
		}

		return bktRoot.Delete(key)
	})
	if errors.Is(err, apistatus.ErrObjectNotFound) {
		return logicerr.Wrap(err)
	}

	return err
}

func (x *Peapod) batch(ctx context.Context, fBktRoot func(bktRoot *bbolt.Bucket) error) error {
	if x.readOnly {
		return common.ErrReadOnly
	}

	x.currentBatchMtx.RLock()

	currentBatch := x.currentBatch

	if currentBatch.initErr != nil {
		x.currentBatchMtx.RUnlock()
		return currentBatch.initErr
	}

	// bbolt.Bucket.Put MUST NOT be called concurrently. This is not obvious from
	// the docs, but panic occurs in practice
	currentBatch.bktRootMtx.Lock()
	currentBatch.nonIdle = true
	err := fBktRoot(currentBatch.bktRoot)
	currentBatch.bktRootMtx.Unlock()
	if err != nil {
		x.currentBatchMtx.RUnlock()
		return fmt.Errorf("put object into BoltDB bucket for container: %w", err)
	}

	x.currentBatchMtx.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-currentBatch.chCommitted:
		return currentBatch.commitErr
	}
}

// Iterate iterates over all objects stored in the underlying database and
// passes them into objHandler. Errors are passed into errorHandler if it's
// specified and ignoreErrors is true. If error is returned from handlers
// iteration stops.
//
// Use IterateAddresses to iterate over keys only.
func (x *Peapod) Iterate(objHandler func(addr oid.Address, data []byte, id []byte) error, errorHandler func(addr oid.Address, err error) error, ignoreErrors bool) error {
	return x.iterate(objHandler, errorHandler, nil, ignoreErrors)
}

// IterateLazily is similar to Iterate, but allows to skip/defer object
// retrieval in the handler. Use getter function when needed.
func (x *Peapod) IterateLazily(lazyHandler func(addr oid.Address, getter func() ([]byte, error)) error, ignoreErrors bool) error {
	return x.iterate(nil, nil, lazyHandler, ignoreErrors)
}

func (x *Peapod) iterate(objHandler func(oid.Address, []byte, []byte) error,
	errorHandler func(oid.Address, error) error,
	lazyHandler func(oid.Address, func() ([]byte, error)) error,
	ignoreErrors bool) error {
	var addr oid.Address

	err := x.bolt.View(func(tx *bbolt.Tx) error {
		bktRoot := tx.Bucket(rootBucket)
		if bktRoot == nil {
			return errMissingRootBucket
		}

		return bktRoot.ForEach(func(k, v []byte) error {
			err := decodeKeyForObject(&addr, k)
			if err != nil {
				if ignoreErrors {
					if errorHandler != nil {
						return errorHandler(addr, err)
					}

					return nil
				}

				return fmt.Errorf("decode object address from bucket key: %w", err)
			}

			v, err = x.compress.Decompress(v)
			if err != nil {
				if ignoreErrors {
					if errorHandler != nil {
						return errorHandler(addr, err)
					}

					return nil
				}

				return fmt.Errorf("decompress value for object '%s': %w", addr, err)
			}

			if lazyHandler != nil {
				return lazyHandler(addr, func() ([]byte, error) {
					return v, nil
				})
			}

			return objHandler(addr, v, storageID)
		})
	})
	if err != nil {
		return fmt.Errorf("exec read-only BoltDB transaction: %w", err)
	}

	return nil
}

// IterateAddresses iterates over all objects stored in the underlying database
// and passes their addresses into f. If f returns an error, IterateAddresses
// returns it and breaks.
func (x *Peapod) IterateAddresses(f func(addr oid.Address) error) error {
	var addr oid.Address

	err := x.bolt.View(func(tx *bbolt.Tx) error {
		bktRoot := tx.Bucket(rootBucket)
		if bktRoot == nil {
			return errMissingRootBucket
		}

		return bktRoot.ForEach(func(k, v []byte) error {
			err := decodeKeyForObject(&addr, k)
			if err != nil {
				return fmt.Errorf("decode object address from bucket key: %w", err)
			}

			return f(addr)
		})
	})
	if err != nil {
		return fmt.Errorf("exec read-only BoltDB transaction: %w", err)
	}

	return nil
}
