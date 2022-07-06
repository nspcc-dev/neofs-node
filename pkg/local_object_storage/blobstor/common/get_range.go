package common

import (
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type GetRangePrm struct {
	Address   oid.Address
	Range     objectSDK.Range
	StorageID []byte
}

type GetRangeRes struct {
	Data []byte
}
