package common

import oid "github.com/nspcc-dev/neofs-sdk-go/object/id"

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	Address   oid.Address
	StorageID []byte
}

// ExistsRes groups the resulting values of Exists operation.
type ExistsRes struct {
	Exists bool
}
