package common

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	Address       oid.Address
	BlobovniczaID *blobovnicza.ID
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct{}
