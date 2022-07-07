package common

import oid "github.com/nspcc-dev/neofs-sdk-go/object/id"

// IterationElement represents a unit of elements through which Iterate operation passes.
type IterationElement struct {
	ObjectData []byte
	Address    oid.Address
	StorageID  []byte
}

// IterationHandler is a generic processor of IterationElement.
type IterationHandler func(IterationElement) error

// IteratePrm groups the parameters of Iterate operation.
type IteratePrm struct {
	Handler      IterationHandler
	LazyHandler  func(oid.Address, func() ([]byte, error)) error
	IgnoreErrors bool
	ErrorHandler func(oid.Address, error) error
}

// IterateRes groups the resulting values of Iterate operation.
type IterateRes struct{}
