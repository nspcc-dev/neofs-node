package container

import (
	"github.com/google/uuid"
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

	if len(c.OwnerID().ToV2().GetValue()) != owner.NEO3WalletSize {
		return errors.Wrap(owner.ErrBadID, "incorrect owner identifier")
	}

	if _, err := uuid.FromBytes(c.Nonce()); err != nil {
		return errors.Wrap(err, "incorrect nonce")
	}

	return nil
}
