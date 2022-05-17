package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	"github.com/nspcc-dev/neofs-sdk-go/container"
)

var (
	errNilPolicy          = errors.New("placement policy is nil")
	errRepeatedAttributes = errors.New("repeated attributes found")
	errEmptyAttribute     = errors.New("empty attribute found")
)

// CheckFormat conducts an initial check of the v2 container data.
//
// It is expected that if a container fails this test,
// it will not be approved by the inner ring.
func CheckFormat(c *container.Container) error {
	if c.PlacementPolicy() == nil {
		return errNilPolicy
	}

	if v := c.Version(); v == nil || !version.IsValid(*v) {
		return fmt.Errorf("incorrect version %s", v)
	}

	if c.OwnerID() == nil {
		return errors.New("missing owner")
	}

	if _, err := c.NonceUUID(); err != nil {
		return fmt.Errorf("incorrect nonce: %w", err)
	}

	// check if there are repeated attributes
	attrs := c.Attributes()
	uniqueAttr := make(map[string]struct{}, len(attrs))
	for _, attr := range attrs {
		if _, exists := uniqueAttr[attr.Key()]; exists {
			return errRepeatedAttributes
		}

		if attr.Value() == "" {
			return errEmptyAttribute
		}

		uniqueAttr[attr.Key()] = struct{}{}
	}

	return nil
}
