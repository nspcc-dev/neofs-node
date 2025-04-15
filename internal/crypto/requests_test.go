package crypto_test

import (
	"bytes"
	"math/rand/v2"
	"testing"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	protoacl "github.com/nspcc-dev/neofs-sdk-go/proto/acl"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func assertInvalidRequestSignatureError(t testing.TB, actual error, expected string) {
	require.EqualError(t, actual, "status: code = 1026 message = "+expected)
	var st apistatus.SignatureVerification
	require.ErrorAs(t, actual, &st)
	require.Equal(t, expected, st.Message())
}

func TestVerifyRequestSignatures(t *testing.T) {
	t.Run("correctly signed", func(t *testing.T) {
		err := icrypto.VerifyRequestSignatures(getObjectSignedRequest)
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
			req := proto.Clone(getObjectSignedRequest).(*protoobject.GetRequest)
			req.VerifyHeader = nil
			err := icrypto.VerifyRequestSignatures(req)
			assertInvalidRequestSignatureError(t, err, "missing verification header")
		})
		for _, tc := range invalidOriginalRequestVerificationHeaderTestcases {
			t.Run(tc.name, func(t *testing.T) {
				req := proto.Clone(getObjectSignedRequest).(*protoobject.GetRequest)
				req.MetaHeader = req.MetaHeader.Origin
				req.VerifyHeader = req.VerifyHeader.Origin
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
				{name: "redundant verification header", msg: "incorrect number of verification headers",
					corrupt: func(valid *protoobject.GetRequest) {
						valid.VerifyHeader = &protosession.RequestVerificationHeader{Origin: valid.VerifyHeader}
					},
				},
				{name: "lacking verification header", msg: "incorrect number of verification headers",
					corrupt: func(valid *protoobject.GetRequest) {
						valid.MetaHeader = &protosession.RequestMetaHeader{Origin: valid.MetaHeader}
					},
				},
				{name: "with body signature", msg: "invalid verification header at depth 0: body signature is set in non-origin verification header",
					corrupt: func(valid *protoobject.GetRequest) {
						valid.VerifyHeader.BodySignature = new(refs.Signature)
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					req := proto.Clone(getObjectSignedRequest).(*protoobject.GetRequest)
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
		author, authorPub, err := icrypto.GetRequestAuthor(getObjectSignedRequest.VerifyHeader)
		require.NoError(t, err)
		require.Equal(t, reqAuthorECDSA, author)
		require.Equal(t, reqSignerECDSAPub, authorPub)
	})
	t.Run("invalid", func(t *testing.T) {
		t.Run("nil", func(t *testing.T) {
			req := proto.Clone(getObjectSignedRequest).(*protoobject.GetRequest)
			req.VerifyHeader = nil
			_, _, err := icrypto.GetRequestAuthor(req.VerifyHeader)
			require.EqualError(t, err, "missing verification header")
		})
		t.Run("without body signature", func(t *testing.T) {
			req := proto.Clone(getObjectSignedRequest).(*protoobject.GetRequest)
			req.VerifyHeader = req.VerifyHeader.Origin
			req.VerifyHeader.BodySignature = nil
			_, _, err := icrypto.GetRequestAuthor(req.VerifyHeader)
			require.EqualError(t, err, "missing body signature")
		})
		t.Run("unsupported body signature scheme", func(t *testing.T) {
			req := proto.Clone(getObjectSignedRequest).(*protoobject.GetRequest)
			req.VerifyHeader = req.VerifyHeader.Origin
			req.VerifyHeader.BodySignature.Scheme = 4
			_, _, err := icrypto.GetRequestAuthor(req.VerifyHeader)
			require.EqualError(t, err, "unsupported scheme 4")
		})
	})
}

var (
	reqSignerECDSAPub = []byte{3, 222, 100, 155, 214, 54, 45, 96, 2, 218, 144, 121, 166, 210, 58, 194, 143, 221, 111, 63, 87,
		254, 66, 2, 236, 94, 45, 93, 30, 39, 191, 127, 80}
	reqSignerL2ECDSAPub = []byte{3, 95, 195, 112, 130, 26, 227, 140, 73, 208, 191, 208, 134, 199, 189, 139, 238, 55, 22, 49,
		165, 67, 146, 187, 82, 232, 85, 95, 144, 75, 87, 243, 21}
	reqAuthorECDSA = user.ID{53, 94, 109, 46, 227, 240, 62, 49, 226, 121, 130, 173, 20, 100, 30, 107, 220, 221, 46, 82, 151, 137, 253, 44, 237}
)

var reqMetaHdr = &protosession.RequestMetaHeader{
	Version: &refs.Version{Major: 4012726028, Minor: 3480185720},
	Epoch:   18426399493784435637, Ttl: 360369950,
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

var reqMetaHdrL2 = &protosession.RequestMetaHeader{
	Version: &refs.Version{Major: 4012726028, Minor: 3480185720},
	Epoch:   18426399493784435637, Ttl: 360369950,
	XHeaders: []*protosession.XHeader{
		{Key: "x-header-1-key", Value: "x-header-1-val"},
		{Key: "x-header-2-key", Value: "x-header-2-val"},
	},
	// tokens unset to reduce the code, they are checked at L1
	Origin:      reqMetaHdr,
	MagicNumber: 14001122173143970642,
}

var getObjectRequestBody = &protoobject.GetRequest_Body{
	Address: &refs.Address{
		ContainerId: &refs.ContainerID{Value: []byte("any_container")},
		ObjectId:    &refs.ObjectID{Value: []byte("any_object")},
	},
	Raw: true,
}

var getObjectSignedRequest = &protoobject.GetRequest{
	Body:       getObjectRequestBody,
	MetaHeader: reqMetaHdrL2,
	VerifyHeader: &protosession.RequestVerificationHeader{
		BodySignature: nil,
		MetaSignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerL2ECDSAPub),
			Sign:   []byte{26, 147, 47, 31, 10, 173, 115, 179, 126, 16, 132, 149, 125, 68, 153, 129, 254, 184, 34, 53, 155, 194, 128, 115, 88, 68, 158, 91, 45, 8, 91, 169, 125, 215, 202, 234, 142, 72, 14, 110, 222, 142, 124, 200, 53, 189, 217, 100, 254, 100, 13, 9, 66, 60, 188, 5, 167, 116, 215, 230, 34, 150, 203, 132},
			Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256,
		},
		OriginSignature: &refs.Signature{
			Key:    bytes.Clone(reqSignerL2ECDSAPub),
			Sign:   []byte{175, 192, 13, 37, 185, 173, 75, 11, 49, 178, 102, 150, 37, 208, 1, 158, 69, 252, 242, 121, 204, 220, 170, 117, 103, 250, 194, 218, 212, 144, 245, 177, 56, 67, 189, 182, 12, 122, 241, 4, 187, 154, 253, 56, 24, 138, 16, 103, 143, 203, 29, 228, 136, 33, 49, 245, 30, 165, 111, 23, 117, 149, 149, 228, 242, 157, 202, 93, 66, 215, 69, 103, 197, 232, 107, 147, 246, 192, 177, 158},
			Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT,
		},
		Origin: &protosession.RequestVerificationHeader{
			BodySignature: &refs.Signature{
				Key: bytes.Clone(reqSignerECDSAPub),
				Sign: []byte{4, 54, 181, 48, 83, 197, 23, 131, 0, 233, 48, 96, 155, 28, 68, 0, 189, 120, 251, 60, 163, 5, 136, 106, 63,
					126, 99, 34, 198, 66, 247, 207, 135, 12, 130, 49, 130, 155, 236, 204, 71, 23, 33, 178, 163, 27, 28, 101, 33, 33,
					91, 229, 217, 170, 250, 226, 62, 93, 22, 3, 181, 81, 69, 9, 97},
				Scheme: refs.SignatureScheme_ECDSA_SHA512,
			},
			MetaSignature: &refs.Signature{
				Key: bytes.Clone(reqSignerECDSAPub),
				Sign: []byte{152, 135, 221, 72, 61, 96, 131, 169, 229, 9, 203, 210, 132, 62, 40, 1, 211, 63, 130, 4, 136, 199, 186,
					219, 104, 2, 50, 101, 89, 252, 144, 184, 28, 125, 230, 39, 128, 238, 210, 223, 69, 128, 164, 112, 218, 133,
					80, 96, 19, 169, 156, 125, 250, 99, 197, 152, 73, 74, 15, 152, 186, 168, 170, 189},
				Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256,
			},
			OriginSignature: &refs.Signature{
				Key: bytes.Clone(reqSignerECDSAPub),
				Sign: []byte{232, 128, 107, 75, 64, 63, 81, 149, 215, 6, 170, 132, 68, 181, 142, 100, 169, 242, 40, 227, 12, 103,
					202, 72, 190, 66, 240, 251, 115, 112, 36, 115, 169, 186, 16, 121, 153, 101, 206, 38, 156, 154, 69, 80, 198, 172, 125,
					115, 114, 54, 224, 44, 198, 137, 131, 236, 163, 209, 208, 136, 146, 184, 70, 136, 60, 200, 208, 106, 154, 206, 83,
					44, 222, 202, 169, 116, 157, 3, 5, 181},
				Scheme: refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT,
			},
		},
	},
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
	{name: "public key/prefix/zero", msg: "decode public key from binary: extra data", corrupt: func(valid *refs.Signature) {
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
	{name: "verification header's origin signature/missing", msg: "missing verification header's origin signature", corrupt: func(valid *protosession.RequestVerificationHeader) {
		valid.OriginSignature = nil
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
		}, invalidRequestVerificationHeaderTestcase{
			name: "verification header's origin signature/" + tc.name, msg: "invalid verification header's origin signature: " + tc.msg,
			corrupt: func(valid *protosession.RequestVerificationHeader) { tc.corrupt(valid.OriginSignature) },
		})
	}
}
