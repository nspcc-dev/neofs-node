package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
)

type ownerIDSource interface {
	OwnerID() *owner.ID
}

func (cp *Processor) checkKeyOwnership(ownerIDSrc ownerIDSource, key *keys.PublicKey) error {
	ownerKeys, err := cp.idClient.AccountKeys(ownerIDSrc.OwnerID())
	if err != nil {
		return fmt.Errorf("could not received owner keys %s: %w", ownerIDSrc.OwnerID(), err)
	}

	for _, ownerKey := range ownerKeys {
		if ownerKey.Equal(key) {
			return nil
		}
	}

	return fmt.Errorf("key %s is not tied to the owner of the container", key)
}
