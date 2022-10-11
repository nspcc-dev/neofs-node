package innerring

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/stretchr/testify/require"
)

func randKey() *neofsecdsa.SignerRFC6979 {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("generate key: %v", err))
	}

	return (*neofsecdsa.SignerRFC6979)(k)
}

type testNeoFSIDContract struct {
	err error

	mKeys map[string][]neofscrypto.PublicKey
}

func (x *testNeoFSIDContract) addKey(usr user.ID, key neofsecdsa.PublicKeyRFC6979) {
	if x.mKeys == nil {
		x.mKeys = make(map[string][]neofscrypto.PublicKey, 1)
	}

	strUsr := usr.EncodeToString()

	x.mKeys[strUsr] = append(x.mKeys[strUsr], &key)
}

func (x *testNeoFSIDContract) iterateUserKeys(usr user.ID, f func(neofscrypto.PublicKey) bool) error {
	if x.err != nil {
		return x.err
	}

	for _, k := range x.mKeys[usr.EncodeToString()] {
		if !f(k) {
			break
		}
	}

	return nil
}

func changeSlice(data []byte) []byte {
	res := slice.Copy(data)
	res[0]++
	return res
}

func TestAuthSystem_VerifySignature(t *testing.T) {
	key := randKey()
	keyPub := neofsecdsa.PublicKeyRFC6979(key.PublicKey)
	data := []byte("any data")

	sgn, err := key.Sign(data)
	require.NoError(t, err)

	var usr user.ID
	user.IDFromKey(&usr, key.PublicKey)

	var neoFSID testNeoFSIDContract

	var a authSystem
	a.init(&neoFSID)

	// resolve key, bound, correct signature
	require.NoError(t, a.VerifySignature(usr, data, sgn, &keyPub))

	// resolve key, bound, incorrect signature
	require.Error(t, a.VerifySignature(usr, changeSlice(data), sgn, &keyPub))
	require.Error(t, a.VerifySignature(usr, data, changeSlice(sgn), &keyPub))

	// contract failure
	neoFSID.err = errors.New("any error")
	require.ErrorIs(t, a.VerifySignature(usr, data, sgn, nil), neoFSID.err)

	neoFSID.err = nil

	// contract key, not bound, correct signature
	require.Error(t, a.VerifySignature(usr, data, sgn, nil))

	// contract key, bound, incorrect signature
	neoFSID.addKey(usr, keyPub)

	require.Error(t, a.VerifySignature(usr, changeSlice(data), sgn, nil))
	require.Error(t, a.VerifySignature(usr, data, changeSlice(sgn), nil))

	// contract key, bound, correct signature
	require.NoError(t, a.VerifySignature(usr, data, sgn, nil))
	require.NoError(t, a.VerifySignature(usr, data, sgn, nil))
}
