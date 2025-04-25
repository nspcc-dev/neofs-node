package blobstor

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrNoPlaceFound is returned when object can't be saved to any sub-storage component
// because of the policy.
var ErrNoPlaceFound = logicerr.New("couldn't find a place to store an object")

// PutBatchPrm groups parameters of PutBatch function.
type PutBatchPrm struct {
	Addr oid.Address
	Obj  *objectSDK.Object
	Raw  []byte
}

// Put saves the object in BLOB storage. raw can be nil, in which case obj is
// serialized internally.
//
// If object is "big", BlobStor saves the object in shallow dir.
// Otherwise, BlobStor saves the object in peapod.
func (b *BlobStor) Put(addr oid.Address, obj *objectSDK.Object, raw []byte) error {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if raw == nil {
		// marshal object
		raw = obj.Marshal()
	}

	if b.storage.Policy == nil || b.storage.Policy(obj, raw) {
		err := b.storage.Storage.Put(addr, raw)
		if err != nil {
			return fmt.Errorf("put object to storage: %w", err)
		}

		logOp(b.log, putOp, addr)

		return nil
	}

	return ErrNoPlaceFound
}

// NeedsCompression returns true if the object should be compressed.
// For an object to be compressed 2 conditions must hold:
// 1. Compression is enabled in settings.
// 2. Object MIME Content-Type is allowed for compression.
func (b *BlobStor) NeedsCompression(obj *objectSDK.Object) bool {
	return b.cfg.compression.NeedsCompression(obj)
}

// PutBatch stores a batch of objects in a sub-storage,
// falling back to individual puts if needed.
func (b *BlobStor) PutBatch(objs []PutBatchPrm) error {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	for i := range objs {
		if objs[i].Raw == nil {
			objs[i].Raw = objs[i].Obj.Marshal()
		}
	}

	if !b.canStoreBatch(objs) {
		return ErrNoPlaceFound
	}

	m := make(map[oid.Address][]byte, len(objs))
	for _, obj := range objs {
		m[obj.Addr] = obj.Raw
	}

	err := b.storage.Storage.PutBatch(m)
	if err != nil {
		return fmt.Errorf("put object to storage: %w", err)
	}

	for _, obj := range objs {
		logOp(b.log, putOp, obj.Addr)
	}
	return nil
}

func (b *BlobStor) canStoreBatch(objs []PutBatchPrm) bool {
	if b.storage.Policy == nil {
		return true
	}
	for _, obj := range objs {
		if !b.storage.Policy(obj.Obj, obj.Raw) {
			return false
		}
	}
	return true
}
