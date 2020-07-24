package object

import (
	"context"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
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
		ownerKey := &test.DecodeKey(0).PublicKey

		ownerID, err := refs.NewOwnerID(ownerKey)
		require.NoError(t, err)

		token := new(service.Token)
		token.SetOwnerID(ownerID)

		ownerKeyBytes := crypto.MarshalPublicKey(ownerKey)
		ownerKeyBytes[0]++
		token.SetOwnerKey(ownerKeyBytes)

		req := new(object.PutRequest)
		req.SetToken(token)

		s := new(tokenPreProcessor)

		require.Error(t, s.preProcess(ctx, req))
	})

	t.Run("static verifier error", func(t *testing.T) {
		vErr := errors.New("test error for static verifier")

		ownerKey := &test.DecodeKey(0).PublicKey

		ownerID, err := refs.NewOwnerID(ownerKey)
		require.NoError(t, err)

		token := new(service.Token)
		token.SetOwnerID(ownerID)
		token.SetOwnerKey(crypto.MarshalPublicKey(ownerKey))

		req := new(object.PutRequest)
		req.SetToken(token)

		s := &tokenPreProcessor{
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
