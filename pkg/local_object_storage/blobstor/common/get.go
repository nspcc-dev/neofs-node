package common

import (
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type GetPrm struct {
	Address   oid.Address
	StorageID []byte
	Raw       bool
}

type GetRes struct {
	Object  *objectSDK.Object
	RawData []byte
}
