package key

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "test",
	Run:   func(cmd *cobra.Command, args []string) {},
}

func Test_getOrGenerate(t *testing.T) {
	dir := t.TempDir()

	wallPath := filepath.Join(dir, "wallet.json")
	w, err := wallet.NewWallet(wallPath)
	require.NoError(t, err)

	badWallPath := filepath.Join(dir, "bad_wallet.json")
	require.NoError(t, os.WriteFile(badWallPath, []byte("bad content"), os.ModePerm))

	acc1, err := wallet.NewAccount()
	require.NoError(t, err)
	require.NoError(t, acc1.Encrypt("pass", w.Scrypt))
	w.AddAccount(acc1)

	acc2, err := wallet.NewAccount()
	require.NoError(t, err)
	require.NoError(t, acc2.Encrypt("pass", w.Scrypt))
	acc2.Default = true
	w.AddAccount(acc2)
	require.NoError(t, w.Save())

	keyPath := filepath.Join(dir, "binary.key")
	rawKey, err := keys.NewPrivateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, rawKey.Bytes(), os.ModePerm))

	wifKey, err := keys.NewPrivateKey()
	require.NoError(t, err)

	nep2Key, err := keys.NewPrivateKey()
	require.NoError(t, err)
	nep2, err := keys.NEP2Encrypt(nep2Key, "pass", keys.NEP2ScryptParams())
	require.NoError(t, err)

	in := bytes.NewBuffer(nil)
	input.Terminal = term.NewTerminal(input.ReadWriter{
		Reader: in,
		Writer: io.Discard,
	}, "")

	checkKeyError(t, filepath.Join(dir, "badfile"), ErrFs)
	checkKeyError(t, badWallPath, ErrInvalidKey)

	t.Run("wallet", func(t *testing.T) {
		checkKeyError(t, wallPath, ErrInvalidPassword)

		in.WriteString("invalid\r")
		checkKeyError(t, wallPath, ErrInvalidPassword)

		in.WriteString("pass\r")
		checkKey(t, wallPath, acc2.PrivateKey()) // default account

		viper.Set(commonflags.Account, acc1.Address)
		in.WriteString("pass\r")
		checkKey(t, wallPath, acc1.PrivateKey())

		viper.Set(commonflags.Account, "not an address")
		checkKeyError(t, wallPath, ErrInvalidAddress)

		acc, err := wallet.NewAccount()
		require.NoError(t, err)
		viper.Set(commonflags.Account, acc.Address)
		checkKeyError(t, wallPath, ErrInvalidAddress)
	})

	t.Run("WIF", func(t *testing.T) {
		checkKeyError(t, wifKey.WIF(), ErrFs)
	})

	t.Run("NEP-2", func(t *testing.T) {
		checkKeyError(t, nep2, ErrFs)
	})

	t.Run("raw key", func(t *testing.T) {
		checkKeyError(t, keyPath, ErrInvalidKey)
	})

	t.Run("generate", func(t *testing.T) {
		viper.Set(commonflags.GenerateKey, true)
		actual, err := getOrGenerate(testCmd)
		require.NoError(t, err)
		require.NotNil(t, actual)
		for _, p := range []*keys.PrivateKey{nep2Key, rawKey, wifKey, acc1.PrivateKey(), acc2.PrivateKey()} {
			require.NotEqual(t, p, actual, "expected new key to be generated")
		}
	})
	t.Run("generate implicitly", func(t *testing.T) {
		viper.Set(commonflags.GenerateKey, false)
		viper.Set(commonflags.WalletPath, "")
		actual, err := getOrGenerate(testCmd)
		require.NoError(t, err)
		require.NotNil(t, actual)
		for _, p := range []*keys.PrivateKey{nep2Key, rawKey, wifKey, acc1.PrivateKey(), acc2.PrivateKey()} {
			require.NotEqual(t, p, actual, "expected new key to be generated")
		}
	})
}

func checkKeyError(t *testing.T, desc string, err error) {
	viper.Set(commonflags.WalletPath, desc)
	_, actualErr := getOrGenerate(testCmd)
	require.ErrorIs(t, actualErr, err)
}

func checkKey(t *testing.T, desc string, expected *keys.PrivateKey) {
	viper.Set(commonflags.WalletPath, desc)
	actual, err := getOrGenerate(testCmd)
	require.NoError(t, err)
	require.Equal(t, &expected.PrivateKey, actual)
}
