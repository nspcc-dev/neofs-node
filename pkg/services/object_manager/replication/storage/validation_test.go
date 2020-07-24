package storage

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	verifier2 "github.com/nspcc-dev/neofs-node/pkg/services/object_manager/verifier"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/test"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testEntity struct {
	err error
}

func (s *testEntity) Verify(context.Context, *object.Object) error { return s.err }

func (s *testEntity) SelfAddr() (multiaddr.Multiaddr, error)                  { panic("implement me") }
func (s *testEntity) Put(context.Context, *localstore.Object) error           { panic("implement me") }
func (s *testEntity) Get(localstore.Address) (*localstore.Object, error)      { panic("implement me") }
func (s *testEntity) Del(localstore.Address) error                            { panic("implement me") }
func (s *testEntity) Meta(localstore.Address) (*localstore.ObjectMeta, error) { panic("implement me") }
func (s *testEntity) Has(localstore.Address) (bool, error)                    { panic("implement me") }
func (s *testEntity) ObjectsCount() (uint64, error)                           { panic("implement me") }
func (s *testEntity) Size() int64                                             { panic("implement me") }
func (s *testEntity) Iterate(localstore.FilterPipeline, localstore.MetaHandler) error {
	panic("implement me")
}

func (s *testEntity) PRead(ctx context.Context, addr refs.Address, rng object.Range) ([]byte, error) {
	panic("implement me")
}

func TestNewObjectValidator(t *testing.T) {
	validParams := ObjectValidatorParams{
		Logger:       zap.L(),
		AddressStore: new(testEntity),
		Localstore:   new(testEntity),
		Verifier:     new(testEntity),
	}

	t.Run("valid params", func(t *testing.T) {
		s, err := NewObjectValidator(&validParams)
		require.NoError(t, err)
		require.NotNil(t, s)
	})
	t.Run("fail on empty local storage", func(t *testing.T) {
		p := validParams
		p.Localstore = nil
		_, err := NewObjectValidator(&p)
		require.EqualError(t, err, errors.Wrap(errEmptyLocalstore, objectValidatorInstanceFailMsg).Error())
	})
	t.Run("fail on empty logger", func(t *testing.T) {
		p := validParams
		p.Logger = nil
		_, err := NewObjectValidator(&p)
		require.EqualError(t, err, errors.Wrap(logger.ErrNilLogger, objectValidatorInstanceFailMsg).Error())
	})
}

func TestNewLocalIntegrityVerifier(t *testing.T) {
	var (
		err      error
		verifier verifier2.Verifier
	)

	verifier, err = NewLocalHeadIntegrityVerifier()
	require.NoError(t, err)
	require.NotNil(t, verifier)

	verifier, err = NewLocalIntegrityVerifier()
	require.NoError(t, err)
	require.NotNil(t, verifier)
}

func TestLocalHeadIntegrityVerifier_Verify(t *testing.T) {
	var (
		ctx               = context.TODO()
		ownerPrivateKey   = test.DecodeKey(0)
		ownerPublicKey    = &ownerPrivateKey.PublicKey
		sessionPrivateKey = test.DecodeKey(1)
		sessionPublicKey  = &sessionPrivateKey.PublicKey
	)

	ownerID, err := refs.NewOwnerID(ownerPublicKey)
	require.NoError(t, err)

	s, err := NewLocalIntegrityVerifier()
	require.NoError(t, err)

	okItems := []func() *object.Object{
		// correct object w/ session token
		func() *object.Object {
			token := new(service.Token)
			token.SetOwnerID(ownerID)
			token.SetSessionKey(crypto.MarshalPublicKey(sessionPublicKey))

			require.NoError(t,
				service.AddSignatureWithKey(
					ownerPrivateKey,
					service.NewSignedSessionToken(token),
				),
			)

			obj := new(object.Object)
			obj.AddHeader(&object.Header{
				Value: &object.Header_Token{
					Token: token,
				},
			})

			obj.SetPayload([]byte{1, 2, 3})
			addPayloadChecksum(obj)

			addHeadersChecksum(t, obj, sessionPrivateKey)

			return obj
		},
		// correct object w/o session token
		func() *object.Object {
			obj := new(object.Object)
			obj.SystemHeader.OwnerID = ownerID
			obj.SetPayload([]byte{1, 2, 3})

			addPayloadChecksum(obj)

			obj.AddHeader(&object.Header{
				Value: &object.Header_PublicKey{
					PublicKey: &object.PublicKey{
						Value: crypto.MarshalPublicKey(ownerPublicKey),
					},
				},
			})

			addHeadersChecksum(t, obj, ownerPrivateKey)

			return obj
		},
	}

	failItems := []func() *object.Object{}

	for _, item := range okItems {
		require.NoError(t, s.Verify(ctx, item()))
	}

	for _, item := range failItems {
		require.Error(t, s.Verify(ctx, item()))
	}
}

func addPayloadChecksum(obj *object.Object) {
	payloadChecksum := sha256.Sum256(obj.GetPayload())

	obj.AddHeader(&object.Header{
		Value: &object.Header_PayloadChecksum{
			PayloadChecksum: payloadChecksum[:],
		},
	})
}

func addHeadersChecksum(t *testing.T, obj *object.Object, key *ecdsa.PrivateKey) {
	headersData, err := verifier2.MarshalHeaders(obj, len(obj.Headers))
	require.NoError(t, err)

	headersChecksum := sha256.Sum256(headersData)

	integrityHdr := new(object.IntegrityHeader)
	integrityHdr.SetHeadersChecksum(headersChecksum[:])

	require.NoError(t, service.AddSignatureWithKey(key, integrityHdr))

	obj.AddHeader(&object.Header{
		Value: &object.Header_Integrity{
			Integrity: integrityHdr,
		},
	})
}

func TestPayloadVerifier_Verify(t *testing.T) {
	ctx := context.TODO()
	verifier := new(payloadVerifier)

	t.Run("missing header", func(t *testing.T) {
		obj := new(object.Object)
		require.EqualError(t, verifier.Verify(ctx, obj), errMissingPayloadChecksumHeader.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		payload := testData(t, 10)

		cs := sha256.Sum256(payload)
		hdr := &object.Header_PayloadChecksum{PayloadChecksum: cs[:]}

		obj := &object.Object{
			Headers: []object.Header{{Value: hdr}},
			Payload: payload,
		}

		require.NoError(t, verifier.Verify(ctx, obj))

		hdr.PayloadChecksum[0]++
		require.EqualError(t, verifier.Verify(ctx, obj), errWrongPayloadChecksum.Error())

		hdr.PayloadChecksum[0]--
		obj.Payload[0]++
		require.EqualError(t, verifier.Verify(ctx, obj), errWrongPayloadChecksum.Error())
	})
}

func TestLocalIntegrityVerifier_Verify(t *testing.T) {
	ctx := context.TODO()
	obj := new(object.Object)

	t.Run("head verification failure", func(t *testing.T) {
		hErr := errors.New("test error for head verifier")

		s := &localIntegrityVerifier{
			headVerifier: &testEntity{
				err: hErr, // force head verifier to return hErr
			},
		}

		require.EqualError(t, s.Verify(ctx, obj), hErr.Error())
	})

	t.Run("correct result", func(t *testing.T) {
		pErr := errors.New("test error for payload verifier")

		s := &localIntegrityVerifier{
			headVerifier: new(testEntity),
			payloadVerifier: &testEntity{
				err: pErr, // force payload verifier to return hErr
			},
		}

		require.EqualError(t, s.Verify(ctx, obj), pErr.Error())
	})
}

// testData returns size bytes of random data.
func testData(t *testing.T, size int) []byte {
	res := make([]byte, size)
	_, err := rand.Read(res)
	require.NoError(t, err)
	return res
}

// TODO: write functionality tests
