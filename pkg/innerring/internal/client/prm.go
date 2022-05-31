package neofsapiclient

import (
	"context"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type contextPrm struct {
	ctx context.Context
}

// SetContext sets context.Context used for network communication.
func (x *contextPrm) SetContext(ctx context.Context) {
	x.ctx = ctx
}

type objectAddressPrm struct {
	objAddr oid.Address
}

// SetAddress sets address of the object.
func (x *objectAddressPrm) SetAddress(addr oid.Address) {
	x.objAddr = addr
}

type getObjectPrm struct {
	contextPrm
	objectAddressPrm
}
