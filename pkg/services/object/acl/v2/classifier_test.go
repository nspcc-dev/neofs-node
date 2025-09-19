package v2

import (
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type nopIR struct{}

func (nopIR) InnerRingKeys() ([][]byte, error) {
	return nil, nil
}

type nopFSChain struct {
	FSChain
}

func (nopFSChain) InContainerInLastTwoEpochs(cid.ID, []byte) (bool, error) {
	return false, nil
}

func BenchmarkClassifierLoggerProduction(b *testing.B) {
	l, err := zap.NewProduction()
	require.NoError(b, err)
	require.NotEqual(b, l.Level(), zap.DebugLevel)

	cnrID := cidtest.ID()
	cnrOwner := usertest.ID()
	reqOwner := usertest.OtherID(cnrOwner)
	reqAuthorPub := neofscryptotest.Signer().PublicKeyBytes
	c := senderClassifier{
		log:       l,
		innerRing: nopIR{},
		fsChain:   nopFSChain{},
	}

	for b.Loop() {
		role, err := c.classify(cnrID, cnrOwner, reqOwner, reqAuthorPub)
		require.NoError(b, err)
		require.Equal(b, role, acl.RoleOthers)
	}
}
