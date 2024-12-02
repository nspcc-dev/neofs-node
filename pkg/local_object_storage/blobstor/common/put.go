package common

import (
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	Address oid.Address
	Object  *objectSDK.Object
	RawData []byte
}

// PutRes groups the resulting values of Put operation.
type PutRes struct {
	StorageID []byte
}
