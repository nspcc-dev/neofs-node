package internal

import (
	"bytes"

	session "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
)

// VerifyResponseKeyV2 checks if response is signed with expected key. Returns client.ErrWrongPublicKey if not.
func VerifyResponseKeyV2(expectedKey []byte, resp interface {
	GetVerifyHeader() *session.ResponseVerificationHeader
}) error {
	if !bytes.Equal(resp.GetVerifyHeader().GetBodySignature().GetKey(), expectedKey) {
		return client.ErrWrongPublicKey
	}

	return nil
}
