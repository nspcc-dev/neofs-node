package blobstor

import (
	"fmt"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	rwObject
}

// PutRes groups the resulting values of Put operation.
type PutRes struct {
	roBlobovniczaID
}

// Put saves the object in BLOB storage.
//
// If object is "big", BlobStor saves the object in shallow dir.
// Otherwise, BlobStor saves the object in blobonicza. In this
// case the identifier of blobovnicza is returned.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (b *BlobStor) Put(prm *PutPrm) (*PutRes, error) {
	// marshal object
	data, err := prm.obj.Marshal()
	if err != nil {
		return nil, fmt.Errorf("could not marshal the object: %w", err)
	}

	return b.PutRaw(object.AddressOf(prm.obj), data, b.NeedsCompression(prm.obj))
}

// NeedsCompression returns true if the object should be compressed.
// For an object to be compressed 2 conditions must hold:
// 1. Compression is enabled in settings.
// 2. Object MIME Content-Type is allowed for compression.
func (b *BlobStor) NeedsCompression(obj *objectSDK.Object) bool {
	if !b.compressionEnabled || len(b.uncompressableContentTypes) == 0 {
		return b.compressionEnabled
	}

	for _, attr := range obj.Attributes() {
		if attr.Key() == objectSDK.AttributeContentType {
			for _, value := range b.uncompressableContentTypes {
				match := false
				switch {
				case len(value) > 0 && value[len(value)-1] == '*':
					match = strings.HasPrefix(attr.Value(), value[:len(value)-1])
				case len(value) > 0 && value[0] == '*':
					match = strings.HasSuffix(attr.Value(), value[1:])
				default:
					match = attr.Value() == value
				}
				if match {
					return false
				}
			}
		}
	}

	return b.compressionEnabled
}

// PutRaw saves an already marshaled object in BLOB storage.
func (b *BlobStor) PutRaw(addr *addressSDK.Address, data []byte, compress bool) (*PutRes, error) {
	big := b.isBig(data)

	if big {
		var err error
		if compress {
			err = b.fsTree.PutStream(addr, func(f *os.File) error {
				enc, _ := zstd.NewWriter(f) // nil error if no options are provided
				if _, err := enc.Write(data); err != nil {
					return err
				}
				return enc.Close()
			})
		} else {
			err = b.fsTree.Put(addr, data)
		}
		if err != nil {
			return nil, err
		}

		storagelog.Write(b.log, storagelog.AddressField(addr), storagelog.OpField("fstree PUT"))

		return new(PutRes), nil
	}

	if compress {
		data = b.compressor(data)
	}

	// save object in blobovnicza
	res, err := b.blobovniczas.put(addr, data)
	if err != nil {
		return nil, err
	}

	return &PutRes{
		roBlobovniczaID: roBlobovniczaID{
			blobovniczaID: res,
		},
	}, nil
}

// checks if object is "big".
func (b *BlobStor) isBig(data []byte) bool {
	return uint64(len(data)) > b.smallSizeLimit
}
