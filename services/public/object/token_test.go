package object

import (
	"context"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/stretchr/testify/require"
)

// Entity for mocking interfaces.
// Implementation of any interface intercepts arguments via f (if not nil).
// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
type testTokenEntity struct {
	// Set of interfaces which testCommonEntity must implement, but some methods from those does not call.

	// Argument interceptor. Used for ascertain of correct parameter passage between components.
	f func(...interface{})
	// Mocked result of any interface.
	res interface{}
	// Mocked error of any interface.
	err error
}

func (s testTokenEntity) VerifyKey(_ context.Context, p core.OwnerKeyContainer) error {
	if s.f != nil {
		s.f(p)
	}
	return s.err
}

func (s testTokenEntity) Epoch() uint64 {
	return s.res.(uint64)
}

func (s testTokenEntity) verifySessionToken(_ context.Context, token service.SessionToken) error {
	if s.f != nil {
		s.f(token)
	}
	return s.err
}

func TestTokenPreProcessor(t *testing.T) {
	ctx := context.TODO()

	t.Run("nil token", func(t *testing.T) {
		var req serviceRequest = new(object.PutRequest)
		require.Nil(t, req.GetSessionToken())

		s := new(tokenPreProcessor)

		require.NoError(t, s.preProcess(ctx, req))
	})

	t.Run("forbidden spawn", func(t *testing.T) {
		token := new(service.Token)

		req := new(object.PutRequest)
		req.SetToken(token)

		token.SetVerb(service.Token_Info_Get)

		s := new(tokenPreProcessor)

		require.EqualError(t, s.preProcess(ctx, req), errForbiddenSpawn.Error())
	})

	t.Run("owner key verifier failure", func(t *testing.T) {
		verifierErr := errors.New("test error for key verifier")

		owner := OwnerID{1, 2, 3}
		token := new(service.Token)
		token.SetOwnerID(owner)

		req := new(object.PutRequest)
		req.SetToken(token)

		s := &tokenPreProcessor{
			keyVerifier: &testTokenEntity{
				f: func(items ...interface{}) {
					require.Equal(t, token, items[0])
				},
				err: verifierErr,
			},
		}

		require.EqualError(t, s.preProcess(ctx, req), verifierErr.Error())
	})

	t.Run("static verifier error", func(t *testing.T) {
		vErr := errors.New("test error for static verifier")

		owner := OwnerID{1, 2, 3}
		token := new(service.Token)
		token.SetOwnerID(owner)

		req := new(object.PutRequest)
		req.SetToken(token)

		s := &tokenPreProcessor{
			keyVerifier: new(testTokenEntity),
			staticVerifier: &testTokenEntity{
				f: func(items ...interface{}) {
					require.Equal(t, token, items[0])
				},
				err: vErr,
			},
		}

		require.EqualError(t, s.preProcess(ctx, req), vErr.Error())
	})
}

func TestTokenEpochsVerifier(t *testing.T) {
	ctx := context.TODO()

	t.Run("created after expiration", func(t *testing.T) {
		token := new(service.Token)
		token.SetExpirationEpoch(1)
		token.SetCreationEpoch(token.ExpirationEpoch() + 1)

		s := new(tokenEpochsVerifier)

		require.EqualError(t, s.verifySessionToken(ctx, token), errCreatedAfterExpiration.Error())
	})

	t.Run("expired token", func(t *testing.T) {
		token := new(service.Token)
		token.SetExpirationEpoch(1)

		s := &tokenEpochsVerifier{
			epochRecv: &testTokenEntity{
				res: token.ExpirationEpoch() + 1,
			},
		}

		require.EqualError(t, s.verifySessionToken(ctx, token), errTokenExpired.Error())
	})

	t.Run("valid token", func(t *testing.T) {
		token := new(service.Token)
		token.SetCreationEpoch(1)
		token.SetExpirationEpoch(token.CreationEpoch() + 1)

		s := &tokenEpochsVerifier{
			epochRecv: &testTokenEntity{
				res: token.ExpirationEpoch() - 1,
			},
		}

		require.NoError(t, s.verifySessionToken(ctx, token))
	})
}
