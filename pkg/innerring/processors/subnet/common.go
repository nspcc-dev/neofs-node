package subnetevents

import (
	"fmt"

	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
)

// common interface of subnet notifications with subnet ID.
type eventWithID interface {
	// ReadID reads identifier of the subnet.
	ReadID(*subnetid.ID) error
}

// an error which is returned on zero subnet operation attempt.
type zeroSubnetOp struct {
	op string
}

func (x zeroSubnetOp) Error() string {
	return fmt.Sprintf("zero subnet %s", x.op)
}
