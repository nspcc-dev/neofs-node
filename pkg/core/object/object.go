package object

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

// AddressOf returns the address of the object.
func AddressOf(obj *object.Object) *addressSDK.Address {
	if obj != nil {
		addr := addressSDK.NewAddress()
		addr.SetObjectID(obj.ID())
		addr.SetContainerID(obj.ContainerID())

		return addr
	}

	return nil
}
