package object

import (
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

// AddressOf returns the address of the object.
func AddressOf(obj *object.Object) *addressSDK.Address {
	if obj != nil {
		addr := addressSDK.NewAddress()

		id, ok := obj.ID()
		if ok {
			addr.SetObjectID(id)
		}

		cnr, ok := obj.ContainerID()
		if ok {
			addr.SetContainerID(cnr)
		}

		return addr
	}

	return nil
}
