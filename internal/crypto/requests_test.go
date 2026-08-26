package crypto_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"math/rand/v2"
	"testing"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	protoacl "github.com/nspcc-dev/neofs-sdk-go/proto/acl"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

func assertInvalidRequestSignatureError(t testing.TB, actual error, expected string) {
	require.EqualError(t, actual, "status: code = 1026 message = "+expected)
	var st apistatus.SignatureVerification
	require.ErrorAs(t, actual, &st)
	require.Equal(t, expected, st.Message())
}

func TestVerifyRequestSignaturesWithContext(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(cryptorand.Reader, cert, cert, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err = x509.ParseCertificate(der)
	require.NoError(t, err)
	info, err := peerauth.NewAuthInfo(credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}})
	require.NoError(t, err)
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: info})
	req := &protoobject.GetRequest{MetaHeader: &protosession.RequestMetaHeader{Ttl: 1}}

	require.NoError(t, icrypto.VerifyRequestSignaturesWithContext(ctx, req))
	require.NoError(t, icrypto.VerifyRequestSignaturesN3(ctx, req, nil))

	req.MetaHeader.Ttl = 0
	err = icrypto.VerifyRequestSignaturesWithContext(ctx, req)
	assertInvalidRequestSignatureError(t, err, "missing verification header")
}

func TestVerifyRequestSignatures(t *testing.T) {
	t.Run("correctly signed", func(t *testing.T) {
		t.Run("sig=3", func(t *testing.T) {
			err := icrypto.VerifyRequestSignatures(getObjectRequest3Sig)
			require.NoError(t, err)
		})
		t.Run("sig=2", func(t *testing.T) {
			err := icrypto.VerifyRequestSignatures(getObjectRequest2Sig)
			require.NoError(t, err)
		})
		err := icrypto.VerifyRequestSignatures(getObjectRequest1Sig)
		require.NoError(t, err)
	})
	t.Run("invalid", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			t.Run("untyped", func(t *testing.T) {
				require.Panics(t, func() {
					_ = icrypto.VerifyRequestSignatures[*protoobject.GetRequest_Body](nil)
				})
			})
			t.Run("typed", func(t *testing.T) {
				err := icrypto.VerifyRequestSignatures((*protoobject.GetRequest)(nil))
				assertInvalidRequestSignatureError(t, err, "missing verification header")
			})
		})
		t.Run("without verification header", func(t *testing.T) {
			req := proto.Clone(getObjectRequest3Sig).(*protoobject.GetRequest)
			req.VerifyHeader = nil
			err := icrypto.VerifyRequestSignatures(req)
			assertInvalidRequestSignatureError(t, err, "missing verification header")
		})
		for _, tc := range invalidOriginalRequestVerificationHeaderTestcases {
			t.Run(tc.name, func(t *testing.T) {
				req := proto.Clone(getObjectRequest3Sig).(*protoobject.GetRequest)
				tc.corrupt(req.VerifyHeader)
				err := icrypto.VerifyRequestSignatures(req)
				assertInvalidRequestSignatureError(t, err, "invalid verification header at depth 0: "+tc.msg)

				t.Run("resigned", func(t *testing.T) {
					req := &protoobject.GetRequest{
						Body:         req.Body,
						MetaHeader:   &protosession.RequestMetaHeader{Origin: req.MetaHeader},
						VerifyHeader: req.VerifyHeader,
					}
					req.VerifyHeader, err = neofscrypto.SignRequestWithBuffer(neofscryptotest.Signer(), req, nil)
					require.NoError(t, err)

					err := icrypto.VerifyRequestSignatures(req)
					assertInvalidRequestSignatureError(t, err, "invalid verification header at depth 1: "+tc.msg)
				})
			})
		}
		t.Run("resigned", func(t *testing.T) {
			for _, tc := range []struct {
				name, msg string
				corrupt   func(valid *protoobject.GetRequest)
			}{
				{name: "missing meta signature", msg: "invalid verification header at depth 0: missing meta header's signature",
					corrupt: func(valid *protoobject.GetRequest) {
						valid.VerifyHeader.MetaSignature = nil
					},
				},
				{name: "lacking verification header", msg: "incorrect number of verification headers",
					corrupt: func(valid *protoobject.GetRequest) {
						valid.MetaHeader = &protosession.RequestMetaHeader{Origin: valid.MetaHeader}
					},
				},
				{name: "invalid body signature", msg: "invalid verification header at depth 0: invalid body signature: missing public key",
					corrupt: func(valid *protoobject.GetRequest) {
						valid.VerifyHeader.BodySignature = new(refs.Signature)
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					req := proto.Clone(getObjectRequest3Sig).(*protoobject.GetRequest)
					tc.corrupt(req)
					err := icrypto.VerifyRequestSignatures(req)
					assertInvalidRequestSignatureError(t, err, tc.msg)
				})
			}
		})
	})
}

func TestGetRequestAuthor(t *testing.T) {
	t.Run("correctly signed", func(t *testing.T) {
		t.Run("sig=3", func(t *testing.T) {
			author, authorPub, err := icrypto.GetRequestAuthor(getObjectRequest3Sig.VerifyHeader)
			require.NoError(t, err)
			require.Equal(t, reqAuthorECDSA, author)
			require.Equal(t, reqSignerECDSAPub, authorPub)
		})
		t.Run("sig=2", func(t *testing.T) {
			author, authorPub, err := icrypto.GetRequestAuthor(getObjectRequest2Sig.VerifyHeader)
			require.NoError(t, err)
			require.Equal(t, reqAuthorECDSA, author)
			require.Equal(t, reqSignerECDSAPub, authorPub)
		})

		author, authorPub, err := icrypto.GetRequestAuthor(getObjectRequest1Sig.VerifyHeader)
		require.NoError(t, err)
		require.Equal(t, reqAuthorECDSA, author)
		require.Equal(t, reqSignerECDSAPub, authorPub)
	})
	t.Run("invalid", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			req := proto.Clone(getObjectRequest3Sig).(*protoobject.GetRequest)
			req.VerifyHeader = nil
			_, _, err := icrypto.GetRequestAuthor(req.VerifyHeader)
			require.EqualError(t, err, "missing verification header")
		})
		t.Run("without body signature", func(t *testing.T) {
			req := proto.Clone(getObjectRequest3Sig).(*protoobject.GetRequest)
			req.VerifyHeader.BodySignature = nil
			_, _, err := icrypto.GetRequestAuthor(req.VerifyHeader)
			require.EqualError(t, err, "missing body/request signature")
		})
		t.Run("unsupported body signature scheme", func(t *testing.T) {
			req := proto.Clone(getObjectRequest3Sig).(*protoobject.GetRequest)
			req.VerifyHeader.BodySignature.Scheme = 4
			_, _, err := icrypto.GetRequestAuthor(req.VerifyHeader)
			require.EqualError(t, err, "unsupported scheme 4")
		})
	})
}

// for [elliptic.P256] curve private key: []byte{132, 165, 11, 252, 197, 62, 187, 54, 191, 216, 225, 107, 17, 64, 134, 159, 136, 176, 78, 27, 219, 7, 87, 25, 87, 95, 31, 99, 195, 144, 43, 206}.
var (
	reqSignerECDSAPub = []byte{2, 213, 197, 196, 65, 80, 242, 120, 147, 200, 1, 235, 129, 39, 215, 78, 245, 4, 165, 26, 235, 248, 34, 224, 177, 177, 128, 230, 32, 119, 171, 91, 38}
	reqAuthorECDSA    = user.ID{53, 13, 143, 242, 251, 155, 85, 8, 24, 244, 236, 79, 154, 11, 67, 192, 142, 168, 236, 157, 183, 169, 109, 30, 231}
)

var reqMetaHdrCommon = &protosession.RequestMetaHeader{
	Epoch: 18426399493784435637, Ttl: 360369950,
	XHeaders: []*protosession.XHeader{
		{Key: "x-header-1-key", Value: "x-header-1-val"},
		{Key: "x-header-2-key", Value: "x-header-2-val"},
	},
	SessionToken: &protosession.SessionToken{
		Body: &protosession.SessionToken_Body{
			Id:      []byte("any_ID"),
			OwnerId: &refs.OwnerID{Value: []byte("any_session_owner")},
			Lifetime: &protosession.SessionToken_Body_TokenLifetime{
				Exp: 9296388864757340046, Nbf: 7616299382059580946, Iat: 7881369180031591601,
			},
			SessionKey: []byte("any_session_key"),
			Context: &protosession.SessionToken_Body_Object{
				Object: &protosession.ObjectSessionContext{
					Verb: 598965377,
					Target: &protosession.ObjectSessionContext_Target{
						Container: &refs.ContainerID{Value: []byte("any_target_container")},
						Objects: []*refs.ObjectID{
							{Value: []byte("any_target_object_1")},
							{Value: []byte("any_target_object_2")},
						},
					},
				},
			},
		},
		Signature: &refs.Signature{Key: []byte("any_pub"), Sign: []byte("any_sig"), Scheme: 598965377},
	},
	BearerToken: &protoacl.BearerToken{
		Body: &protoacl.BearerToken_Body{
			EaclTable: &protoacl.EACLTable{
				Version:     &refs.Version{Major: 318436066, Minor: 2840436841},
				ContainerId: &refs.ContainerID{Value: []byte("any_eACL_container")},
				Records: []*protoacl.EACLRecord{
					{Operation: 1119884853, Action: 62729415, Filters: []*protoacl.EACLRecord_Filter{
						{HeaderType: 623516729, MatchType: 1738829273, Key: "filter-1-1-key", Value: "filter-1-1-val"},
						{HeaderType: 1607116959, MatchType: 1367966035, Key: "filter-1-2-key", Value: "filter-1-2-val"},
					}, Targets: []*protoacl.EACLRecord_Target{
						{Role: 611878932, Keys: [][]byte{[]byte("subj-1-1-1"), []byte("subj-1-1-2")}},
						{Role: 1862775306, Keys: [][]byte{[]byte("subj-1-2-1"), []byte("subj-1-2-2")}},
					}},
					{Operation: 1240073398, Action: 1717003574, Filters: []*protoacl.EACLRecord_Filter{
						{HeaderType: 623516729, MatchType: 1738829273, Key: "filter-2-1-key", Value: "filter-2-1-val"},
						{HeaderType: 1607116959, MatchType: 1367966035, Key: "filter-2-2-key", Value: "filter-2-2-val"},
					}, Targets: []*protoacl.EACLRecord_Target{
						{Role: 611878932, Keys: [][]byte{[]byte("subj-2-1-1"), []byte("subj-2-1-2")}},
						{Role: 1862775306, Keys: [][]byte{[]byte("subj-2-2-1"), []byte("subj-2-2-2")}},
					}},
				},
			},
			OwnerId: &refs.OwnerID{Value: []byte("any_bearer_user")},
			Lifetime: &protoacl.BearerToken_Body_TokenLifetime{
				Exp: 13260042237062625207, Nbf: 8718573876473538197, Iat: 2028326755325539864},
			Issuer: &refs.OwnerID{Value: []byte("any_bearer_issuer")},
		},
		Signature: &refs.Signature{Key: []byte("any_pub"), Sign: []byte("any_sig"), Scheme: 1375722142},
	},
	MagicNumber: 14001122173143970642,
}

var getObjectRequestBody = &protoobject.GetRequest_Body{
	Address: &refs.Address{
		ContainerId: &refs.ContainerID{Value: []byte("any_container")},
		ObjectId:    &refs.ObjectID{Value: []byte("any_object")},
	},
	Raw: true,
}

var getObjectRequest3Sig = &protoobject.GetRequest{
	Body: getObjectRequestBody,
	VerifyHeader: &protosession.RequestVerificationHeader{
		BodySignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerECDSAPub),
			Sign:   []byte{4, 56, 79, 165, 147, 25, 162, 55, 146, 162, 174, 1, 108, 163, 133, 122, 91, 130, 188, 29, 110, 126, 164, 144, 126, 109, 168, 11, 158, 168, 155, 191, 115, 7, 139, 70, 236, 169, 138, 187, 199, 48, 101, 211, 75, 147, 133, 31, 53, 226, 181, 107, 13, 107, 205, 236, 156, 193, 25, 207, 118, 116, 34, 20, 182},
			Scheme: refs.SignatureScheme_ECDSA_SHA512,
		},
		MetaSignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerECDSAPub),
			Sign:   []byte{2, 186, 211, 234, 183, 119, 20, 155, 114, 36, 233, 220, 98, 192, 9, 202, 113, 23, 217, 198, 1, 168, 15, 197, 220, 236, 104, 175, 240, 26, 211, 212, 97, 228, 109, 175, 2, 247, 52, 34, 211, 251, 160, 6, 106, 77, 238, 228, 233, 161, 51, 84, 246, 85, 231, 82, 110, 167, 142, 15, 217, 211, 143, 205},
			Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256,
		},
		OriginSignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerECDSAPub),
			Sign:   []byte{9, 43, 7, 208, 176, 187, 111, 33, 31, 49, 28, 55, 182, 36, 86, 250, 95, 140, 24, 18, 239, 84, 82, 2, 131, 7, 100, 30, 59, 209, 165, 116, 185, 35, 60, 217, 192, 208, 212, 217, 15, 167, 32, 161, 28, 18, 111, 234, 37, 221, 118, 88, 162, 123, 57, 41, 12, 168, 184, 164, 245, 81, 207, 120, 64, 93, 254, 2, 186, 114, 249, 26, 92, 165, 236, 29, 32, 62, 239, 45},
			Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT,
		},
	},
}

var getObjectRequest2Sig = &protoobject.GetRequest{
	Body: getObjectRequestBody,
	VerifyHeader: &protosession.RequestVerificationHeader{
		BodySignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerECDSAPub),
			Sign:   []byte{4, 56, 79, 165, 147, 25, 162, 55, 146, 162, 174, 1, 108, 163, 133, 122, 91, 130, 188, 29, 110, 126, 164, 144, 126, 109, 168, 11, 158, 168, 155, 191, 115, 7, 139, 70, 236, 169, 138, 187, 199, 48, 101, 211, 75, 147, 133, 31, 53, 226, 181, 107, 13, 107, 205, 236, 156, 193, 25, 207, 118, 116, 34, 20, 182},
			Scheme: refs.SignatureScheme_ECDSA_SHA512,
		},
		MetaSignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerECDSAPub),
			Sign:   []byte{5, 75, 99, 246, 252, 121, 250, 106, 172, 6, 11, 104, 188, 230, 243, 136, 80, 69, 101, 187, 49, 243, 118, 252, 163, 22, 181, 24, 207, 70, 172, 11, 102, 88, 247, 52, 229, 218, 153, 216, 37, 184, 57, 90, 94, 136, 13, 254, 8, 131, 29, 57, 22, 145, 227, 36, 220, 94, 247, 14, 32, 235, 190, 77},
			Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256,
		},
	},
}

var getObjectRequest1Sig = &protoobject.GetRequest{
	Body: getObjectRequestBody,
	VerifyHeader: &protosession.RequestVerificationHeader{
		RequestSignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerECDSAPub),
			Sign:   []byte{4, 38, 90, 130, 181, 152, 135, 113, 140, 242, 241, 225, 163, 156, 57, 85, 120, 233, 197, 41, 118, 215, 146, 204, 153, 171, 162, 51, 172, 158, 242, 86, 93, 59, 24, 165, 80, 220, 194, 27, 164, 132, 207, 67, 253, 131, 37, 138, 137, 46, 200, 224, 159, 188, 139, 245, 170, 234, 93, 77, 172, 187, 182, 30, 53},
			Scheme: refs.SignatureScheme_ECDSA_SHA512,
		},
	},
}

func init() {
	reqMetaHdr3Sig := proto.Clone(reqMetaHdrCommon).(*protosession.RequestMetaHeader)
	reqMetaHdr3Sig.Version = &refs.Version{Major: 2, Minor: 24}
	getObjectRequest3Sig.MetaHeader = reqMetaHdr3Sig

	reqMetaHdr2Sig := proto.Clone(reqMetaHdrCommon).(*protosession.RequestMetaHeader)
	reqMetaHdr2Sig.Version = &refs.Version{Major: 2, Minor: 25}
	getObjectRequest2Sig.MetaHeader = reqMetaHdr2Sig

	reqMetaHdr1Sig := proto.Clone(reqMetaHdrCommon).(*protosession.RequestMetaHeader)
	reqMetaHdr1Sig.Version = &refs.Version{Major: 2, Minor: 26}
	getObjectRequest1Sig.MetaHeader = reqMetaHdr1Sig
}

var corruptSigTestcases = []struct {
	name, msg string
	corrupt   func(valid *refs.Signature)
}{
	{name: "scheme/negative", msg: "negative scheme -1", corrupt: func(valid *refs.Signature) { valid.Scheme = -1 }},
	{name: "scheme/unsupported ", msg: "unsupported scheme 3", corrupt: func(valid *refs.Signature) { valid.Scheme = 3 }},
	{name: "scheme/other ", msg: "signature mismatch", corrupt: func(valid *refs.Signature) {
		if valid.Scheme++; valid.Scheme >= 3 {
			valid.Scheme = 0
		}
	}},
	{name: "public key/nil", msg: "missing public key", corrupt: func(valid *refs.Signature) { valid.Key = nil }},
	{name: "public key/empty", msg: "missing public key", corrupt: func(valid *refs.Signature) { valid.Key = []byte{} }},
	{name: "public key/undersize", msg: "decode public key from binary: unexpected EOF", corrupt: func(valid *refs.Signature) {
		valid.Key = bytes.Clone(reqSignerECDSAPub)[:32]
	}},
	{name: "public key/oversize", msg: "decode public key from binary: extra data", corrupt: func(valid *refs.Signature) {
		valid.Key = append(bytes.Clone(reqSignerECDSAPub), 1)
	}},
	{name: "public key/prefix/zero", msg: "decode public key from binary: point at infinity is not a valid key", corrupt: func(valid *refs.Signature) {
		valid.Key[0] = 0x00
	}},
	{name: "public key/prefix/unsupported", msg: "decode public key from binary: invalid prefix 5", corrupt: func(valid *refs.Signature) {
		valid.Key[0] = 0x05
	}},
	{name: "public key/prefix/uncompressed in compressed form", msg: "decode public key from binary: EOF", corrupt: func(valid *refs.Signature) {
		valid.Key[0] = 0x04
	}},
	{name: "public key/prefix/other compressed", msg: "signature mismatch", corrupt: func(valid *refs.Signature) {
		if valid.Key[0] == 0x02 {
			valid.Key[0] = 0x03
		} else {
			valid.Key[0] = 0x02
		}
	}},
	{name: "public key/wrong", msg: "signature mismatch", corrupt: func(valid *refs.Signature) {
		valid.Key = neofscryptotest.Signer().PublicKeyBytes
	}},
	{name: "signature/nil", msg: "signature mismatch", corrupt: func(valid *refs.Signature) { valid.Sign = nil }},
	{name: "signature/empty", msg: "signature mismatch", corrupt: func(valid *refs.Signature) { valid.Sign = []byte{} }},
	{name: "signature/nil", msg: "signature mismatch", corrupt: func(valid *refs.Signature) { valid.Sign = nil }},
	{name: "signature/empty", msg: "signature mismatch", corrupt: func(valid *refs.Signature) { valid.Sign = []byte{} }},
	{name: "signature/undersize", msg: "signature mismatch", corrupt: func(valid *refs.Signature) {
		valid.Sign = valid.Sign[:len(valid.Sign)-1]
	}},
	{name: "signature/oversize", msg: "signature mismatch", corrupt: func(valid *refs.Signature) {
		valid.Sign = append(valid.Sign, 1)
	}},
	{name: "signature/one byte change", msg: "signature mismatch", corrupt: func(valid *refs.Signature) {
		valid.Sign[rand.IntN(len(valid.Sign))]++
	}},
	// TODO: uncomment after https://github.com/nspcc-dev/neofs-sdk-go/issues/673
	// {name: "public key/infinite", msg: "signature mismatch", corrupt: func(valid *refs.Signature) {
	// 	valid.Key = []byte{0x00}
	// }},
}

type invalidRequestVerificationHeaderTestcase = struct {
	name, msg string
	corrupt   func(valid *protosession.RequestVerificationHeader)
}

// set in init.
var invalidOriginalRequestVerificationHeaderTestcases = []invalidRequestVerificationHeaderTestcase{
	{name: "body signature/missing", msg: "missing body signature", corrupt: func(valid *protosession.RequestVerificationHeader) {
		valid.BodySignature = nil
	}},
	{name: "meta header signature/missing", msg: "missing meta header's signature", corrupt: func(valid *protosession.RequestVerificationHeader) {
		valid.MetaSignature = nil
	}},
}

func init() {
	for _, tc := range corruptSigTestcases {
		invalidOriginalRequestVerificationHeaderTestcases = append(invalidOriginalRequestVerificationHeaderTestcases, invalidRequestVerificationHeaderTestcase{
			name: "body signature/" + tc.name, msg: "invalid body signature: " + tc.msg,
			corrupt: func(valid *protosession.RequestVerificationHeader) { tc.corrupt(valid.BodySignature) },
		}, invalidRequestVerificationHeaderTestcase{
			name: "meta header signature/" + tc.name, msg: "invalid meta header's signature: " + tc.msg,
			corrupt: func(valid *protosession.RequestVerificationHeader) { tc.corrupt(valid.MetaSignature) },
		})
	}
}
