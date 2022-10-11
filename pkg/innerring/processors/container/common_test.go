package container

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func newPublicKey() *neofsecdsa.PublicKeyRFC6979 {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("generate private key: %v", err))
	}

	return (*neofsecdsa.PublicKeyRFC6979)(&k.PublicKey)
}

func serializePublicKey(pub neofscrypto.PublicKey) []byte {
	binPubKey := make([]byte, pub.MaxEncodedSize())
	return binPubKey[:pub.Encode(binPubKey)]
}

type testAuthSystem struct {
	NeoFS

	t *testing.T

	input *signatureVerificationData

	fail bool
}

func (x testAuthSystem) Authorize(usr user.ID, data []byte, signature []byte, key *neofsecdsa.PublicKeyRFC6979) error {
	require.Equal(x.t, x.input.containerCreator, usr)
	require.Equal(x.t, x.input.signedData, data)
	require.Equal(x.t, x.input.signature, signature)

	if x.input.binPublicKey == nil {
		require.Nil(x.t, key)
	} else {
		require.Equal(x.t, x.input.binPublicKey, serializePublicKey(key))
	}

	if x.fail {
		return errors.New("test signature verification failure")
	}

	return nil
}

func TestVerifySignature(t *testing.T) {
	var err error
	var p Processor

	authSystem := &testAuthSystem{
		t: t,
	}

	p.neoFS = authSystem

	input := signatureVerificationData{
		containerCreator: *usertest.ID(),
		binPublicKey:     serializePublicKey(newPublicKey()),
		signature:        []byte("any signature"),
		signedData:       []byte("any data"),
	}

	authSystem.input = &input

	authSystem.fail = false

	err = p.verifySignature(input)
	require.NoError(t, err)

	authSystem.fail = true

	err = p.verifySignature(input)
	require.Error(t, err)
}
