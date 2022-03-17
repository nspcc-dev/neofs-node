package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	containerIDSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/token"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"
)

const repoRoot = "../../../"

func TestBearer(t *testing.T) {
	p1, _ := keys.NewPrivateKeyFromWIF("Ky9aa3orjw59JPdYBPaUUx4eVNfaKR7ovnUmzDfgV1c1erypq4C9")
	p2, _ := keys.NewPrivateKeyFromWIF("L23RiMvtgNrsqR9i5YiVkMzGWYxyAqh8qyzWu9gYrr8tzTgHa8KR")

	raw, _ := ioutil.ReadFile(repoRoot + "wallet.key")
	p, _ := keys.NewPrivateKeyFromBytes(raw)
	tok := token.NewBearerToken()

	//own := owner.NewIDFromPublicKey((*ecdsa.PublicKey)(p.PublicKey()))
	//tok.SetOwner(own)

	tb := eacl.NewTable()
	cid := containerIDSDK.New()
	require.NoError(t, cid.Parse("6Vuz8pxni4m1GLjwCRJXA2HCPV9XEdsWFKYf45puQKQp"))
	tb.SetCID(cid)

	r := eacl.NewRecord()
	r.SetAction(eacl.ActionAllow)
	r.SetOperation(eacl.OperationPut)
	tgt := eacl.NewTarget()
	tgt.SetRole(eacl.RoleUnknown)
	tgt.SetBinaryKeys([][]byte{p1.PublicKey().Bytes()})
	r.SetTargets(*tgt)
	tb.AddRecord(r)

	r = eacl.NewRecord()
	r.SetAction(eacl.ActionDeny)
	r.SetOperation(eacl.OperationPut)
	tgt = eacl.NewTarget()
	tgt.SetRole(eacl.RoleUnknown)
	tgt.SetBinaryKeys([][]byte{p2.PublicKey().Bytes()})
	r.SetTargets(*tgt)
	tb.AddRecord(r)

	tb.SetVersion(*version.Current())
	tok.SetEACLTable(tb)
	tok.SetLifetime(10000, 0, 0)

	require.NoError(t, tok.SignTokenWC(&p.PrivateKey))
	//tok.Signature().SetScheme(signature.ECDSAWithSHA512)
	data, err := tok.MarshalJSON()
	require.NoError(t, err)
	fmt.Println(p.WIF())
	fmt.Println(string(data))
	require.NoError(t, ioutil.WriteFile(repoRoot+"bearer.json", data, os.ModePerm))
}

func Test_getKey(t *testing.T) {
	dir := t.TempDir()

	wallPath := filepath.Join(dir, "wallet.json")
	w, err := wallet.NewWallet(wallPath)
	require.NoError(t, err)

	acc1, err := wallet.NewAccount()
	require.NoError(t, err)
	require.NoError(t, acc1.Encrypt("pass", keys.NEP2ScryptParams()))
	w.AddAccount(acc1)

	acc2, err := wallet.NewAccount()
	require.NoError(t, err)
	require.NoError(t, acc2.Encrypt("pass", keys.NEP2ScryptParams()))
	acc2.Default = true
	w.AddAccount(acc2)
	require.NoError(t, w.Save())
	w.Close()

	keyPath := filepath.Join(dir, "binary.key")
	rawKey, err := keys.NewPrivateKey()
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(keyPath, rawKey.Bytes(), os.ModePerm))

	wifKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	nep2Key, err := keys.NewPrivateKey()
	require.NoError(t, err)
	nep2, err := keys.NEP2Encrypt(nep2Key, "pass", keys.NEP2ScryptParams())
	require.NoError(t, err)

	in := bytes.NewBuffer(nil)
	input.Terminal = term.NewTerminal(input.ReadWriter{
		Reader: in,
		Writer: ioutil.Discard,
	}, "")

	checkKeyError(t, filepath.Join(dir, "badfile"), errInvalidKey)

	t.Run("wallet", func(t *testing.T) {
		checkKeyError(t, wallPath, errInvalidPassword)

		in.WriteString("invalid\r")
		checkKeyError(t, wallPath, errInvalidPassword)

		in.WriteString("pass\r")
		checkKey(t, wallPath, acc2.PrivateKey()) // default account

		viper.Set(address, acc1.Address)
		in.WriteString("pass\r")
		checkKey(t, wallPath, acc1.PrivateKey())

		viper.Set(address, "not an address")
		checkKeyError(t, wallPath, errInvalidAddress)

		acc, err := wallet.NewAccount()
		require.NoError(t, err)
		viper.Set(address, acc.Address)
		checkKeyError(t, wallPath, errInvalidAddress)
	})

	t.Run("WIF", func(t *testing.T) {
		checkKey(t, wifKey.WIF(), wifKey)
	})

	t.Run("NEP-2", func(t *testing.T) {
		checkKeyError(t, nep2, errInvalidPassword)

		in.WriteString("invalid\r")
		checkKeyError(t, nep2, errInvalidPassword)

		in.WriteString("pass\r")
		checkKey(t, nep2, nep2Key)

		t.Run("password from config", func(t *testing.T) {
			viper.Set(password, "invalid")
			in.WriteString("pass\r")
			checkKeyError(t, nep2, errInvalidPassword)

			viper.Set(password, "pass")
			in.WriteString("invalid\r")
			checkKey(t, nep2, nep2Key)
		})
	})

	t.Run("raw key", func(t *testing.T) {
		checkKey(t, keyPath, rawKey)
	})

	t.Run("generate", func(t *testing.T) {
		viper.Set(generateKey, true)
		actual, err := getKey()
		require.NoError(t, err)
		require.NotNil(t, actual)
		for _, p := range []*keys.PrivateKey{nep2Key, rawKey, wifKey, acc1.PrivateKey(), acc2.PrivateKey()} {
			require.NotEqual(t, p, actual, "expected new key to be generated")
		}
	})
}

func checkKeyError(t *testing.T, desc string, err error) {
	viper.Set(walletPath, desc)
	_, actualErr := getKey()
	require.True(t, errors.Is(actualErr, err), "got: %v", actualErr)
}

func checkKey(t *testing.T, desc string, expected *keys.PrivateKey) {
	viper.Set(walletPath, desc)
	actual, err := getKey()
	require.NoError(t, err)
	require.Equal(t, &expected.PrivateKey, actual)
}
