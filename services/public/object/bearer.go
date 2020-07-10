package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/pkg/errors"
)

type bearerTokenVerifier interface {
	verifyBearerToken(context.Context, CID, service.BearerToken) error
}

type complexBearerVerifier struct {
	items []bearerTokenVerifier
}

type bearerActualityVerifier struct {
	epochRecv EpochReceiver
}

type bearerOwnershipVerifier struct {
	cnrOwnerChecker implementations.ContainerOwnerChecker
}

type bearerSignatureVerifier struct{}

var errWrongBearerOwner = errors.New("bearer author is not a container owner")

func (s complexBearerVerifier) verifyBearerToken(ctx context.Context, cid CID, token service.BearerToken) error {
	for i := range s.items {
		if err := s.items[i].verifyBearerToken(ctx, cid, token); err != nil {
			return err
		}
	}

	return nil
}

func (s bearerActualityVerifier) verifyBearerToken(_ context.Context, _ CID, token service.BearerToken) error {
	local := s.epochRecv.Epoch()
	validUntil := token.ExpirationEpoch()

	if local > validUntil {
		return errors.Errorf("bearer token is expired (local %d, valid until %d)",
			local,
			validUntil,
		)
	}

	return nil
}

func (s bearerOwnershipVerifier) verifyBearerToken(ctx context.Context, cid CID, token service.BearerToken) error {
	isOwner, err := s.cnrOwnerChecker.IsContainerOwner(ctx, cid, token.GetOwnerID())
	if err != nil {
		return err
	} else if !isOwner {
		return errWrongBearerOwner
	}

	return nil
}

func (s bearerSignatureVerifier) verifyBearerToken(_ context.Context, _ CID, token service.BearerToken) error {
	return service.VerifySignatureWithKey(
		crypto.UnmarshalPublicKey(token.GetOwnerKey()),
		service.NewVerifiedBearerToken(token),
	)
}
