package object

import oid "github.com/nspcc-dev/neofs-sdk-go/object/id"

// OIDWithPayloadLen pairs some object's ID and payload length.
type OIDWithPayloadLen struct {
	ID         oid.ID
	PayloadLen uint64
}
