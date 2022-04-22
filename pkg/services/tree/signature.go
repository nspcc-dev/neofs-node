package tree

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

func (s *Service) verifyClient(req interface{}, cid cidSDK.ID, rawKey []byte) error {
	// TODO(@fyrchik): #1328 access control
	return nil
	//nolint:govet
	err := signature.VerifyServiceMessage(req)
	if err != nil {
		return err
	}

	cnr, err := s.cnrSource.Get(cid)
	if err != nil {
		return fmt.Errorf("can't get container %s: %w", cid, err)
	}

	ownerID := cnr.Value.Owner()

	pub, err := keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
	if err != nil {
		return fmt.Errorf("invalid public key: %w", err)
	}

	var actualID user.ID
	user.IDFromKey(&actualID, (ecdsa.PublicKey)(*pub))

	if !actualID.Equals(ownerID) {
		return errors.New("`Move` request must be signed by a container owner")
	}

	return nil
}
