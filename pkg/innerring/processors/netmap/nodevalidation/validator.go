package nodevalidation

import (
	netmapprocessor "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// CompositeValidator wraps netmapprocessor.NodeValidators.
//
// For correct operation, CompositeValidator must be created
// using the constructor (New). After successful creation,
// the CompositeValidator is immediately ready to work through
// API.
type CompositeValidator struct {
	validators []netmapprocessor.NodeValidator
}

// New creates a new instance of the CompositeValidator.
//
// The created CompositeValidator does not require additional
// initialization and is completely ready for work.
func New(validators ...netmapprocessor.NodeValidator) *CompositeValidator {
	return &CompositeValidator{validators}
}

// Verify passes netmap.NodeInfo to wrapped validators.
//
// If error appears, returns it immediately.
func (c *CompositeValidator) Verify(ni netmap.NodeInfo) error {
	for _, v := range c.validators {
		if err := v.Verify(ni); err != nil {
			return err
		}
	}

	return nil
}
