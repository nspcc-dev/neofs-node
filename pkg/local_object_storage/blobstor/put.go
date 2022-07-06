package blobstor

import (
	"fmt"
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// Put saves the object in BLOB storage.
//
// If object is "big", BlobStor saves the object in shallow dir.
// Otherwise, BlobStor saves the object in blobonicza. In this
// case the identifier of blobovnicza is returned.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (b *BlobStor) Put(prm common.PutPrm) (common.PutRes, error) {
	prm.Address = object.AddressOf(prm.Object)
	if prm.RawData == nil {
		// marshal object
		data, err := prm.Object.Marshal()
		if err != nil {
			return common.PutRes{}, fmt.Errorf("could not marshal the object: %w", err)
		}
		prm.RawData = data
	}

	return b.PutRaw(prm, b.NeedsCompression(prm.Object))
}

// NeedsCompression returns true if the object should be compressed.
// For an object to be compressed 2 conditions must hold:
// 1. Compression is enabled in settings.
// 2. Object MIME Content-Type is allowed for compression.
func (b *BlobStor) NeedsCompression(obj *objectSDK.Object) bool {
	return b.cfg.CConfig.NeedsCompression(obj)
}

// PutRaw saves an already marshaled object in BLOB storage.
func (b *BlobStor) PutRaw(prm common.PutPrm, compress bool) (common.PutRes, error) {
	big := b.isBig(prm.RawData)

	if big {
		var err error
		if compress {
			err = b.fsTree.PutStream(prm.Address, func(f *os.File) error {
				enc, _ := zstd.NewWriter(f) // nil error if no options are provided
				if _, err := enc.Write(prm.RawData); err != nil {
					return err
				}
				return enc.Close()
			})
		} else {
			_, err = b.fsTree.Put(prm)
		}
		if err != nil {
			return common.PutRes{}, err
		}

		storagelog.Write(b.log, storagelog.AddressField(prm.Address), storagelog.OpField("fstree PUT"))

		return common.PutRes{}, nil
	}

	if compress {
		prm.RawData = b.CConfig.Compress(prm.RawData)
	}

	// save object in blobovnicza
	return b.blobovniczas.Put(prm)
}

// checks if object is "big".
func (b *BlobStor) isBig(data []byte) bool {
	return uint64(len(data)) > b.smallSizeLimit
}
