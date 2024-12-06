package blobstor

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ErrNoPlaceFound is returned when object can't be saved to any sub-storage component
// because of the policy.
var ErrNoPlaceFound = logicerr.New("couldn't find a place to store an object")

// Put saves the object in BLOB storage. raw can be nil, in which case obj is
// serialized internally.
//
// If object is "big", BlobStor saves the object in shallow dir.
// Otherwise, BlobStor saves the object in peapod.
//
// Returns storage ID that saved the object given or an error if any.
func (b *BlobStor) Put(addr oid.Address, obj *objectSDK.Object, raw []byte) ([]byte, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if raw == nil {
		// marshal object
		raw = obj.Marshal()
	}

	var overflow bool

	for i := range b.storage {
		if b.storage[i].Policy == nil || b.storage[i].Policy(obj, raw) {
			var (
				typ = b.storage[i].Storage.Type()
				err = b.storage[i].Storage.Put(addr, raw)
				sid []byte
			)
			if err != nil {
				if overflow = errors.Is(err, common.ErrNoSpace); overflow {
					b.log.Debug("blobstor sub-storage overflowed, will try another one",
						zap.String("type", typ))
					continue
				}

				return nil, fmt.Errorf("put object to sub-storage %s: %w", typ, err)
			}

			if typ == "peapod" {
				sid = []byte(typ)
			} else {
				sid = []byte{} // Compatibility quirk, https://github.com/nspcc-dev/neofs-node/issues/2888
			}
			logOp(b.log, putOp, addr, typ, sid)

			return sid, nil
		}
	}

	if overflow {
		return nil, common.ErrNoSpace
	}

	return nil, ErrNoPlaceFound
}

// NeedsCompression returns true if the object should be compressed.
// For an object to be compressed 2 conditions must hold:
// 1. Compression is enabled in settings.
// 2. Object MIME Content-Type is allowed for compression.
func (b *BlobStor) NeedsCompression(obj *objectSDK.Object) bool {
	return b.cfg.compression.NeedsCompression(obj)
}
