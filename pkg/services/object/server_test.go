package object_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	objectgrpc "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	refsv2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	. "github.com/nspcc-dev/neofs-node/pkg/services/object"
	v2 "github.com/nspcc-dev/neofs-node/pkg/services/object/acl/v2"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/nspcc-dev/neofs-sdk-go/stat"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

func randECDSAPrivateKey(tb testing.TB) *ecdsa.PrivateKey {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(tb, err)
	return k
}

type noCallObjectService struct{}

func (x noCallObjectService) Get(context.Context, getsvc.Prm) error {
	panic("must not be called")
}

func (x noCallObjectService) Put(context.Context) (*putsvc.Streamer, error) {
	panic("must not be called")
}

func (x noCallObjectService) Head(context.Context, getsvc.HeadPrm) error {
	panic("must not be called")
}

func (x noCallObjectService) Search(context.Context, searchsvc.Prm) error {
	panic("must not be called")
}

func (x noCallObjectService) Delete(context.Context, deletesvc.Prm) error {
	panic("must not be called")
}

func (x noCallObjectService) GetRange(context.Context, getsvc.RangePrm) error {
	panic("must not be called")
}

func (x noCallObjectService) GetRangeHash(context.Context, getsvc.RangeHashPrm) (*getsvc.RangeHashRes, error) {
	panic("must not be called")
}

type noCallTestFSChain struct{}

func (x *noCallTestFSChain) ForEachContainerNodePublicKeyInLastTwoEpochs(cid.ID, func([]byte) bool) error {
	panic("must not be called")
}
func (*noCallTestFSChain) IsOwnPublicKey([]byte) bool      { panic("must not be called") }
func (*noCallTestFSChain) CurrentEpoch() uint64            { panic("must not be called") }
func (*noCallTestFSChain) CurrentBlock() uint32            { panic("must not be called") }
func (*noCallTestFSChain) CurrentEpochDuration() uint64    { panic("must not be called") }
func (*noCallTestFSChain) LocalNodeUnderMaintenance() bool { panic("must not be called") }

type noCallTestStorage struct{}

func (noCallTestStorage) VerifyAndStoreObjectLocally(object.Object) error {
	panic("must not be called")
}
func (noCallTestStorage) GetSessionPrivateKey(user.ID, uuid.UUID) (ecdsa.PrivateKey, error) {
	panic("implement me")
}

type noCallTestACLChecker struct{}

func (noCallTestACLChecker) CheckBasicACL(v2.RequestInfo) bool           { panic("must not be called") }
func (noCallTestACLChecker) CheckEACL(any, v2.RequestInfo) error         { panic("must not be called") }
func (noCallTestACLChecker) StickyBitCheck(v2.RequestInfo, user.ID) bool { panic("must not be called") }

type noCallTestReqInfoExtractor struct{}

func (noCallTestReqInfoExtractor) PutRequestToInfo(*objectgrpc.PutRequest) (v2.RequestInfo, user.ID, error) {
	panic("must not be called")
}
func (noCallTestReqInfoExtractor) DeleteRequestToInfo(*objectgrpc.DeleteRequest) (v2.RequestInfo, error) {
	panic("must not be called")
}
func (noCallTestReqInfoExtractor) HeadRequestToInfo(*objectgrpc.HeadRequest) (v2.RequestInfo, error) {
	panic("must not be called")
}
func (noCallTestReqInfoExtractor) HashRequestToInfo(*objectgrpc.GetRangeHashRequest) (v2.RequestInfo, error) {
	panic("must not be called")
}
func (noCallTestReqInfoExtractor) GetRequestToInfo(*objectgrpc.GetRequest) (v2.RequestInfo, error) {
	panic("must not be called")
}
func (noCallTestReqInfoExtractor) RangeRequestToInfo(*objectgrpc.GetRangeRequest) (v2.RequestInfo, error) {
	panic("must not be called")
}
func (noCallTestReqInfoExtractor) SearchRequestToInfo(*objectgrpc.SearchRequest) (v2.RequestInfo, error) {
	panic("must not be called")
}

type nopACLChecker struct{}

func (nopACLChecker) CheckBasicACL(v2.RequestInfo) bool           { return true }
func (nopACLChecker) CheckEACL(any, v2.RequestInfo) error         { return nil }
func (nopACLChecker) StickyBitCheck(v2.RequestInfo, user.ID) bool { return true }

type nopReqInfoExtractor struct{}

func (nopReqInfoExtractor) PutRequestToInfo(*objectgrpc.PutRequest) (v2.RequestInfo, user.ID, error) {
	return v2.RequestInfo{}, user.ID{}, nil
}
func (nopReqInfoExtractor) DeleteRequestToInfo(*objectgrpc.DeleteRequest) (v2.RequestInfo, error) {
	return v2.RequestInfo{}, nil
}
func (nopReqInfoExtractor) HeadRequestToInfo(*objectgrpc.HeadRequest) (v2.RequestInfo, error) {
	return v2.RequestInfo{}, nil
}
func (nopReqInfoExtractor) HashRequestToInfo(*objectgrpc.GetRangeHashRequest) (v2.RequestInfo, error) {
	return v2.RequestInfo{}, nil
}
func (nopReqInfoExtractor) GetRequestToInfo(*objectgrpc.GetRequest) (v2.RequestInfo, error) {
	return v2.RequestInfo{}, nil
}
func (nopReqInfoExtractor) RangeRequestToInfo(*objectgrpc.GetRangeRequest) (v2.RequestInfo, error) {
	return v2.RequestInfo{}, nil
}
func (nopReqInfoExtractor) SearchRequestToInfo(*objectgrpc.SearchRequest) (v2.RequestInfo, error) {
	return v2.RequestInfo{}, nil
}

type nopMetrics struct{}

func (nopMetrics) HandleOpExecResult(stat.Method, bool, time.Duration) {}
func (nopMetrics) AddPutPayload(int)                                   {}
func (nopMetrics) AddGetPayload(int)                                   {}

type testFSChain struct {
	nopFSChain
	tb testing.TB

	// server state
	serverPubKey []byte

	// request data
	clientPubKey []byte
	cnr          cid.ID

	// return
	cnrErr           error
	clientOutsideCnr bool
	serverOutsideCnr bool
}

func newTestFSChain(tb testing.TB, serverPubKey, clientPubKey []byte, cnr cid.ID) *testFSChain {
	return &testFSChain{
		tb:           tb,
		serverPubKey: serverPubKey,
		clientPubKey: clientPubKey,
		cnr:          cnr,
	}
}

func (x *testFSChain) ForEachContainerNodePublicKeyInLastTwoEpochs(cnr cid.ID, f func(pubKey []byte) bool) error {
	require.True(x.tb, cnr == x.cnr)
	require.NotNil(x.tb, f)
	if x.cnrErr != nil {
		return x.cnrErr
	}
	if !x.clientOutsideCnr && !f(x.clientPubKey) {
		return nil
	}
	if !x.serverOutsideCnr && !f(x.serverPubKey) {
		return nil
	}
	return nil
}

func (x *testFSChain) IsOwnPublicKey(pubKey []byte) bool { return bytes.Equal(x.serverPubKey, pubKey) }

func (x *testFSChain) CurrentEpoch() uint64 { return 0 }

func (*testFSChain) LocalNodeUnderMaintenance() bool { return false }

type testStorage struct {
	t testing.TB
	// request data
	obj *objectgrpc.Object
	// return
	storeErr error
}

func newTestStorage(t testing.TB, obj *objectgrpc.Object) *testStorage {
	return &testStorage{t: t, obj: obj}
}

func (x *testStorage) VerifyAndStoreObjectLocally(obj object.Object) error {
	require.Equal(x.t, x.obj, obj.ToV2().ToGRPCMessage().(*objectgrpc.Object))
	return x.storeErr
}

func (x *testStorage) GetSessionPrivateKey(user.ID, uuid.UUID) (ecdsa.PrivateKey, error) {
	return ecdsa.PrivateKey{}, apistatus.ErrSessionTokenNotFound
}

func anyValidRequest(tb testing.TB, signer neofscrypto.Signer, cnr cid.ID, objID oid.ID) (*objectgrpc.ReplicateRequest, object.Object) {
	obj := objecttest.Object()
	obj.SetType(object.TypeRegular)
	obj.SetContainerID(cnr)
	obj.SetID(objID)
	obj.SetFirstID(oidtest.ID())
	obj.SetPreviousID(oidtest.ID())

	sig, err := signer.Sign(objID[:])
	require.NoError(tb, err)

	req := &objectgrpc.ReplicateRequest{
		Object: obj.ToV2().ToGRPCMessage().(*objectgrpc.Object),
		Signature: &refs.Signature{
			Key:  neofscrypto.PublicKeyBytes(signer.Public()),
			Sign: sig,
		},
		SignObject: false,
	}

	switch signer.Scheme() {
	default:
		tb.Fatalf("unsupported scheme %v", signer.Scheme())
	case neofscrypto.ECDSA_SHA512:
		req.Signature.Scheme = refs.SignatureScheme_ECDSA_SHA512
	case neofscrypto.ECDSA_DETERMINISTIC_SHA256:
		req.Signature.Scheme = refs.SignatureScheme_ECDSA_RFC6979_SHA256
	case neofscrypto.ECDSA_WALLETCONNECT:
		req.Signature.Scheme = refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT
	}

	return req, obj
}

func TestServer_Replicate(t *testing.T) {
	var noCallFSChain noCallTestFSChain
	var noCallObjSvc noCallObjectService
	var noCallStorage noCallTestStorage
	var noCallACLChecker noCallTestACLChecker
	var noCallReqProc noCallTestReqInfoExtractor
	noCallSrv := New(noCallObjSvc, 0, &noCallFSChain, noCallStorage, neofscryptotest.Signer().ECDSAPrivateKey, nopMetrics{}, noCallACLChecker, noCallReqProc)
	clientSigner := neofscryptotest.Signer()
	clientPubKey := neofscrypto.PublicKeyBytes(clientSigner.Public())
	serverPubKey := neofscrypto.PublicKeyBytes(neofscryptotest.Signer().Public())
	cnr := cidtest.ID()
	objID := oidtest.ID()
	req, _ := anyValidRequest(t, clientSigner, cnr, objID)

	t.Run("invalid/unsupported signature format", func(t *testing.T) {
		// note: verification is tested separately
		for _, tc := range []struct {
			name         string
			fSig         func() *refs.Signature
			expectedCode uint32
			expectedMsg  string
		}{
			{
				name:         "missing object signature field",
				fSig:         func() *refs.Signature { return nil },
				expectedCode: 1024,
				expectedMsg:  "missing object signature field",
			},
			{
				name: "missing public key field in the signature field",
				fSig: func() *refs.Signature {
					return &refs.Signature{
						Key:    nil,
						Sign:   []byte("any non-empty"),
						Scheme: refs.SignatureScheme_ECDSA_SHA512, // any supported
					}
				},
				expectedCode: 1024,
				expectedMsg:  "public key field is missing/empty in the object signature field",
			},
			{
				name: "missing value field in the signature field",
				fSig: func() *refs.Signature {
					return &refs.Signature{
						Key:    []byte("any non-empty"),
						Sign:   []byte{},
						Scheme: refs.SignatureScheme_ECDSA_SHA512, // any supported
					}
				},
				expectedCode: 1024,
				expectedMsg:  "signature value is missing/empty in the object signature field",
			},
			{
				name: "unsupported scheme in the signature field",
				fSig: func() *refs.Signature {
					return &refs.Signature{
						Key:    []byte("any non-empty"),
						Sign:   []byte("any non-empty"),
						Scheme: 3,
					}
				},
				expectedCode: 1024,
				expectedMsg:  "unsupported scheme in the object signature field",
			},
		} {
			req, _ := anyValidRequest(t, neofscryptotest.Signer(), cidtest.ID(), oidtest.ID())
			req.Signature = tc.fSig()
			resp, err := noCallSrv.Replicate(context.Background(), req)
			require.NoError(t, err, tc.name)
			require.EqualValues(t, tc.expectedCode, resp.GetStatus().GetCode(), tc.name)
			require.Equal(t, tc.expectedMsg, resp.GetStatus().GetMessage(), tc.name)
		}
	})

	t.Run("signature verification failure", func(t *testing.T) {
		// note: common format is tested separately
		for _, tc := range []struct {
			name         string
			fSig         func(bObj []byte) *refs.Signature
			expectedCode uint32
			expectedMsg  string
		}{
			{
				name: "ECDSA SHA-512: invalid public key",
				fSig: func(_ []byte) *refs.Signature {
					return &refs.Signature{
						Key:    []byte("not ECDSA key"),
						Sign:   []byte("any non-empty"),
						Scheme: refs.SignatureScheme_ECDSA_SHA512,
					}
				},
				expectedCode: 1024,
				expectedMsg:  "invalid ECDSA public key in the object signature field",
			},
			{
				name: "ECDSA SHA-512: signature mismatch",
				fSig: func(bObj []byte) *refs.Signature {
					return &refs.Signature{
						Key:    neofscrypto.PublicKeyBytes((*neofsecdsa.Signer)(randECDSAPrivateKey(t)).Public()),
						Sign:   []byte("definitely invalid"),
						Scheme: refs.SignatureScheme_ECDSA_SHA512,
					}
				},
				expectedCode: 1024,
				expectedMsg:  "signature mismatch in the object signature field",
			},
			{
				name: "ECDSA SHA-256 deterministic: invalid public key",
				fSig: func(_ []byte) *refs.Signature {
					return &refs.Signature{
						Key:    []byte("not ECDSA key"),
						Sign:   []byte("any non-empty"),
						Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256,
					}
				},
				expectedCode: 1024,
				expectedMsg:  "invalid ECDSA public key in the object signature field",
			},
			{
				name: "ECDSA SHA-256 deterministic: signature mismatch",
				fSig: func(bObj []byte) *refs.Signature {
					return &refs.Signature{
						Key:    neofscrypto.PublicKeyBytes((*neofsecdsa.SignerRFC6979)(randECDSAPrivateKey(t)).Public()),
						Sign:   []byte("definitely invalid"),
						Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256,
					}
				},
				expectedCode: 1024,
				expectedMsg:  "signature mismatch in the object signature field",
			},
			{
				name: "ECDSA SHA-256 WalletConnect: invalid public key",
				fSig: func(_ []byte) *refs.Signature {
					return &refs.Signature{
						Key:    []byte("not ECDSA key"),
						Sign:   []byte("any non-empty"),
						Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT,
					}
				},
				expectedCode: 1024,
				expectedMsg:  "invalid ECDSA public key in the object signature field",
			},
			{
				name: "ECDSA SHA-256 WalletConnect: signature mismatch",
				fSig: func(bObj []byte) *refs.Signature {
					return &refs.Signature{
						Key:    neofscrypto.PublicKeyBytes((*neofsecdsa.SignerWalletConnect)(randECDSAPrivateKey(t)).Public()),
						Sign:   []byte("definitely invalid"),
						Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT,
					}
				},
				expectedCode: 1024,
				expectedMsg:  "signature mismatch in the object signature field",
			},
		} {
			obj := objecttest.Object()
			bObj := obj.Marshal()

			resp, err := noCallSrv.Replicate(context.Background(), &objectgrpc.ReplicateRequest{
				Object:    obj.ToV2().ToGRPCMessage().(*objectgrpc.Object),
				Signature: tc.fSig(bObj),
			})
			require.NoError(t, err, tc.name)
			require.EqualValues(t, tc.expectedCode, resp.GetStatus().GetCode(), tc.name)
			require.Equal(t, tc.expectedMsg, resp.GetStatus().GetMessage(), tc.name)
		}
	})

	t.Run("apply storage policy failure", func(t *testing.T) {
		fsChain := newTestFSChain(t, serverPubKey, clientPubKey, cnr)
		srv := New(noCallObjSvc, 0, fsChain, noCallStorage, neofscryptotest.Signer().ECDSAPrivateKey, nopMetrics{}, noCallACLChecker, noCallReqProc)

		fsChain.cnrErr = errors.New("any error")

		resp, err := srv.Replicate(context.Background(), req)
		require.NoError(t, err)
		require.EqualValues(t, 1024, resp.GetStatus().GetCode())
		require.Equal(t, "failed to apply object's storage policy: any error", resp.GetStatus().GetMessage())
	})

	t.Run("client or server mismatches object's storage policy", func(t *testing.T) {
		fsChain := newTestFSChain(t, serverPubKey, clientPubKey, cnr)
		srv := New(noCallObjSvc, 0, fsChain, noCallStorage, neofscryptotest.Signer().ECDSAPrivateKey, nopMetrics{}, noCallACLChecker, noCallReqProc)

		fsChain.serverOutsideCnr = true
		fsChain.clientOutsideCnr = true

		resp, err := srv.Replicate(context.Background(), req)
		require.NoError(t, err)
		require.EqualValues(t, 2048, resp.GetStatus().GetCode())
		require.Equal(t, "server does not match the object's storage policy", resp.GetStatus().GetMessage())

		fsChain.serverOutsideCnr = false

		resp, err = srv.Replicate(context.Background(), req)
		require.NoError(t, err)
		require.EqualValues(t, 2048, resp.GetStatus().GetCode())
		require.Equal(t, "client does not match the object's storage policy", resp.GetStatus().GetMessage())
	})

	t.Run("local storage failure", func(t *testing.T) {
		fsChain := newTestFSChain(t, serverPubKey, clientPubKey, cnr)
		s := newTestStorage(t, req.Object)
		srv := New(noCallObjSvc, 0, fsChain, s, neofscryptotest.Signer().ECDSAPrivateKey, nopMetrics{}, noCallACLChecker, noCallReqProc)

		s.storeErr = errors.New("any error")

		resp, err := srv.Replicate(context.Background(), req)
		require.NoError(t, err)
		require.EqualValues(t, 1024, resp.GetStatus().GetCode())
		require.Equal(t, "failed to verify and store object locally: any error", resp.GetStatus().GetMessage())
	})

	t.Run("meta information signature", func(t *testing.T) {
		var mNumber uint32 = 123
		signer := neofscryptotest.Signer()
		reqForSignature, o := anyValidRequest(t, clientSigner, cnr, objID)
		fsChain := newTestFSChain(t, serverPubKey, clientPubKey, cnr)
		s := newTestStorage(t, reqForSignature.Object)
		srv := New(noCallObjSvc, mNumber, fsChain, s, signer.ECDSAPrivateKey, nopMetrics{}, noCallACLChecker, noCallReqProc)

		t.Run("signature not requested", func(t *testing.T) {
			resp, err := srv.Replicate(context.Background(), reqForSignature)
			require.NoError(t, err)
			require.EqualValues(t, 0, resp.GetStatus().GetCode())
			require.Empty(t, resp.GetStatus().GetMessage())
			require.Empty(t, resp.GetObjectSignature())
		})

		t.Run("signature is requested", func(t *testing.T) {
			reqForSignature.SignObject = true

			resp, err := srv.Replicate(context.Background(), reqForSignature)
			require.NoError(t, err)
			require.EqualValues(t, 0, resp.GetStatus().GetCode())
			require.Empty(t, resp.GetStatus().GetMessage())
			require.NotNil(t, resp.GetObjectSignature())

			sigsRaw := resp.GetObjectSignature()

			for i := range 3 {
				var sigV2 refsv2.Signature
				l := binary.LittleEndian.Uint32(sigsRaw)

				require.NoError(t, sigV2.Unmarshal(sigsRaw[4:4+l]))

				var sig neofscrypto.Signature
				require.NoError(t, sig.ReadFromV2(sigV2))

				require.Equal(t, signer.PublicKeyBytes, sig.PublicKeyBytes())
				require.True(t, sig.Verify(objectcore.EncodeReplicationMetaInfo(
					o.GetContainerID(), o.GetID(), o.GetFirstID(), o.GetPreviousID(), o.PayloadSize(), o.Type(), nil, nil,
					uint64((123+1+i)*240), mNumber)), fmt.Sprintf("wrong %d signature", i+1))

				sigsRaw = sigsRaw[4+l:]
			}
		})
	})

	t.Run("OK", func(t *testing.T) {
		fsChain := newTestFSChain(t, serverPubKey, clientPubKey, cnr)
		s := newTestStorage(t, req.Object)
		srv := New(noCallObjSvc, 0, fsChain, s, neofscryptotest.Signer().ECDSAPrivateKey, nopMetrics{}, noCallACLChecker, noCallReqProc)

		resp, err := srv.Replicate(context.Background(), req)
		require.NoError(t, err)
		require.EqualValues(t, 0, resp.GetStatus().GetCode())
		require.Empty(t, resp.GetStatus().GetMessage())
	})
}

type nopFSChain struct{}

func (nopFSChain) CurrentEpoch() uint64 {
	return 123
}

func (nopFSChain) CurrentBlock() uint32 {
	return 123 * 240
}

func (nopFSChain) CurrentEpochDuration() uint64 {
	return 240
}

func (x nopFSChain) ForEachContainerNodePublicKeyInLastTwoEpochs(cid.ID, func(pubKey []byte) bool) error {
	return nil
}

func (x nopFSChain) IsOwnPublicKey([]byte) bool {
	return false
}

func (nopFSChain) LocalNodeUnderMaintenance() bool { return false }

type nopStorage struct{}

func (nopStorage) VerifyAndStoreObjectLocally(object.Object) error { return nil }
func (nopStorage) GetSessionPrivateKey(user.ID, uuid.UUID) (ecdsa.PrivateKey, error) {
	return ecdsa.PrivateKey{}, apistatus.ErrSessionTokenNotFound
}

func BenchmarkServer_Replicate(b *testing.B) {
	ctx := context.Background()
	var fsChain nopFSChain

	srv := New(nil, 0, fsChain, nopStorage{}, neofscryptotest.Signer().ECDSAPrivateKey, nopMetrics{}, nopACLChecker{}, nopReqInfoExtractor{})

	for _, tc := range []struct {
		name      string
		newSigner func(tb testing.TB) neofscrypto.Signer
	}{
		{
			name: "ECDSA SHA-512",
			newSigner: func(tb testing.TB) neofscrypto.Signer {
				return (*neofsecdsa.Signer)(randECDSAPrivateKey(tb))
			},
		},
		{
			name: "ECDSA SHA-256 deterministic",
			newSigner: func(tb testing.TB) neofscrypto.Signer {
				return (*neofsecdsa.SignerRFC6979)(randECDSAPrivateKey(tb))
			},
		},
		{
			name: "ECDSA SHA-256 WalletConnect",
			newSigner: func(tb testing.TB) neofscrypto.Signer {
				return (*neofsecdsa.SignerWalletConnect)(randECDSAPrivateKey(tb))
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			req, _ := anyValidRequest(b, tc.newSigner(b), cidtest.ID(), oidtest.ID())

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				resp, err := srv.Replicate(ctx, req)
				require.NoError(b, err)
				require.Zero(b, resp.GetStatus().GetCode())
			}
		})
	}
}
