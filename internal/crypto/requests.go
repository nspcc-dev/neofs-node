package crypto

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neofs-node/pkg/network/peerauth"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	protoencoding "github.com/nspcc-dev/neofs-sdk-go/proto/encoding"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// VerifyRequestSignatures checks whether all request signatures are set and
// valid. Returns [apistatus.SignatureVerification] otherwise.
func VerifyRequestSignatures[B protoencoding.Message](req neofscrypto.SignedRequest[B]) error {
	return verifyRequestSignatures(req, nil)
}

// VerifyRequestSignaturesWithContext is same as [VerifyRequestSignatures], but
// skips verification for an authenticated inter-node request with TTL equal to one.
func VerifyRequestSignaturesWithContext[B protoencoding.Message](ctx context.Context, req neofscrypto.SignedRequest[B]) error {
	if !requestNeedsSignature(ctx, req) {
		return nil
	}
	return verifyRequestSignatures(req, nil)
}

func verifyRequestSignatures[B protoencoding.Message](req neofscrypto.SignedRequest[B], verifyN3 func(data, invocScript, verifScript []byte) error) error {
	err := neofscrypto.VerifyRequestWithBufferN3(req, nil, verifyN3)
	if err != nil {
		var st apistatus.SignatureVerification
		st.SetMessage(err.Error())
		return st
	}
	return nil
}

// VerifyRequestSignaturesN3 is same as [VerifyRequestSignatures] but supports
// [neofscrypto.N3] scheme.
func VerifyRequestSignaturesN3[B protoencoding.Message](ctx context.Context, req neofscrypto.SignedRequest[B], fsChain N3ScriptRunner) error {
	if !requestNeedsSignature(ctx, req) {
		return nil
	}
	return verifyRequestSignatures(req, func(data, invocScript, verifScript []byte) error {
		verifScriptHash := hash.Hash160(verifScript)
		return verifyN3ScriptsNow(fsChain, verifScriptHash, invocScript, verifScript, func() [sha256.Size]byte {
			return sha256.Sum256(data)
		})
	})
}

func requestNeedsSignature[B protoencoding.Message](ctx context.Context, req neofscrypto.SignedRequest[B]) bool {
	if req.GetVerifyHeader() != nil {
		return true
	}
	meta := req.GetMetaHeader()
	if meta == nil || meta.GetTtl() != 1 {
		return true
	}
	return !peerauth.IsTrustedPeer(ctx)
}

// GetRequestAuthor returns ID of the request author along with public key from
// the request verification header. meta is the request's meta header: which
// signature carries verified authorship depends on the request's declared API
// version, exactly as in neofs-sdk-go's request signature verification, so
// the two must never be decided independently.
func GetRequestAuthor(vh *protosession.RequestVerificationHeader, meta *protosession.RequestMetaHeader) (user.ID, []byte, error) {
	if vh == nil {
		return user.ID{}, nil, errors.New("missing verification header")
	}

	var sig *refs.Signature
	if singleRequestSignature(meta) {
		// For this API version, neofs-sdk-go verifies RequestSignature only
		// (see multipleReqSignatures in neofs-sdk-go/crypto/proto.go);
		// BodySignature carries no verified authentication and must be
		// ignored for identity, or a caller could pass verification with
		// their own key while naming an arbitrary victim as the author.
		sig = vh.RequestSignature
		if sig == nil {
			return user.ID{}, nil, errors.New("missing request signature")
		}
	} else {
		sig = vh.BodySignature
		if sig == nil {
			if vh.RequestSignature == nil {
				return user.ID{}, nil, errors.New("missing body/request signature")
			}
			sig = vh.RequestSignature
		}
	}

	switch sig.Scheme {
	default:
		return user.ID{}, nil, fmt.Errorf("unsupported scheme %v", sig.Scheme)
	case refs.SignatureScheme_ECDSA_SHA512, refs.SignatureScheme_ECDSA_RFC6979_SHA256, refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT:
		// TODO: being called with VerifyRequestSignatures, public key is decoded twice.
		//  Not so big overhead, but still better to avoid this
		ecdsaPub, _ := decodeECDSAPublicKey(sig.Key)
		return user.NewFromECDSAPublicKey(*ecdsaPub), sig.Key, nil
	case refs.SignatureScheme_N3:
		return user.NewFromScriptHash(hash.Hash160(sig.Key)), sig.Key, nil
	}
}

// singleRequestSignature reports whether a request declaring this meta header
// is, per the NeoFS API protocol, authenticated by a single RequestSignature
// rather than the older per-field signature set. This must mirror
// multipleReqSignatures in neofs-sdk-go/crypto/proto.go (negated): that is the
// function that actually decides, during signature verification, which
// field(s) are checked.
func singleRequestSignature(m *protosession.RequestMetaHeader) bool {
	return m != nil && m.Version != nil &&
		(m.Version.Major > 2 || (m.Version.Major == 2 && m.Version.Minor >= 26))
}
