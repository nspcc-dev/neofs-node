package common

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type GetRangePrm struct {
	Address       oid.Address
	Range         objectSDK.Range
	BlobovniczaID *blobovnicza.ID
}

type GetRangeRes struct {
	Data []byte
}
