package internal

import (
	"bytes"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
)

// VerifyResponseKeyV2 checks if response is signed with expected key. Returns client.ErrWrongPublicKey if not.
func VerifyResponseKeyV2(expectedKey []byte, resp interface {
	GetVerifyHeader() *protosession.ResponseVerificationHeader
}) error {
	if !bytes.Equal(resp.GetVerifyHeader().GetBodySignature().GetKey(), expectedKey) {
		return client.ErrWrongPublicKey
	}

	return nil
}
