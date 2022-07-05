package common

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type GetPrm struct {
	Address       oid.Address
	BlobovniczaID *blobovnicza.ID
}

type GetRes struct {
	Object *objectSDK.Object
}
