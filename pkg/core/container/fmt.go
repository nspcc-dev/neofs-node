package container

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/pkg/errors"
)

var errNilPolicy = errors.New("placement policy is nil")

// CheckFormat conducts an initial check of the v2 container data.
//
// It is expected that if a container fails this test,
// it will not be inner-ring approved.
func CheckFormat(c *container.Container) error {
	if c.PlacementPolicy() == nil {
		return errNilPolicy
	}

	if err := pkg.IsSupportedVersion(c.Version()); err != nil {
		return errors.Wrap(err, "incorrect version")
	}

	if ln := len(c.OwnerID().ToV2().GetValue()); ln != owner.NEO3WalletSize {
		return errors.Errorf("incorrect owner identifier: expected length %d != %d", owner.NEO3WalletSize, ln)
	}

	if _, err := c.NonceUUID(); err != nil {
		return errors.Wrap(err, "incorrect nonce")
	}

	return nil
}
