package session

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/session"
)

var errExpiredSession = errors.New("expired session")

func (s sessionService) Create(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
	// check lifetime
	expired := req.ExpirationEpoch()
	if s.epochReceiver.Epoch() > expired {
		return nil, errExpiredSession
	}

	// generate private token for session
	pToken, err := session.NewPrivateToken(expired)
	if err != nil {
		return nil, err
	}

	pkBytes, err := session.PublicSessionToken(pToken)
	if err != nil {
		return nil, err
	}

	// generate token ID
	tokenID, err := refs.NewUUID()
	if err != nil {
		return nil, err
	}

	// create private token storage key
	pTokenKey := session.PrivateTokenKey{}
	pTokenKey.SetOwnerID(req.GetOwnerID())
	pTokenKey.SetTokenID(tokenID)

	// store private token
	if err := s.ts.Store(pTokenKey, pToken); err != nil {
		return nil, err
	}

	// construct response
	resp := new(session.CreateResponse)
	resp.SetID(tokenID)
	resp.SetSessionKey(pkBytes)

	return resp, nil
}
