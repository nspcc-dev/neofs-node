package morph

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"
)

const testContractPassword = "grouppass"

func TestGenerateAlphabet(t *testing.T) {
	const size = 4

	walletDir := t.TempDir()
	buf := setupTestTerminal(t)

	cmd := generateAlphabetCmd
	v := viper.GetViper()

	t.Run("zero size", func(t *testing.T) {
		buf.Reset()
		v.Set(alphabetWalletsFlag, walletDir)
		require.NoError(t, cmd.Flags().Set(alphabetSizeFlag, "0"))
		buf.WriteString("pass\r")
		require.Error(t, generateAlphabetCreds(cmd, nil))
	})
	t.Run("no password provided", func(t *testing.T) {
		buf.Reset()
		v.Set(alphabetWalletsFlag, walletDir)
		require.NoError(t, cmd.Flags().Set(alphabetSizeFlag, "1"))
		require.Error(t, generateAlphabetCreds(cmd, nil))
	})
	t.Run("missing directory", func(t *testing.T) {
		buf.Reset()
		dir := filepath.Join(os.TempDir(), "notexist."+strconv.FormatUint(rand.Uint64(), 10))
		v.Set(alphabetWalletsFlag, dir)
		require.NoError(t, cmd.Flags().Set(alphabetSizeFlag, "1"))
		buf.WriteString("pass\r")
		require.Error(t, generateAlphabetCreds(cmd, nil))
	})
	t.Run("no password for contract group wallet", func(t *testing.T) {
		buf.Reset()
		v.Set(alphabetWalletsFlag, walletDir)
		require.NoError(t, cmd.Flags().Set(alphabetSizeFlag, strconv.FormatUint(size, 10)))
		for i := uint64(0); i < size; i++ {
			buf.WriteString(strconv.FormatUint(i, 10) + "\r")
		}
		require.Error(t, generateAlphabetCreds(cmd, nil))
	})

	buf.Reset()
	v.Set(alphabetWalletsFlag, walletDir)
	require.NoError(t, generateAlphabetCmd.Flags().Set(alphabetSizeFlag, strconv.FormatUint(size, 10)))
	for i := uint64(0); i < size; i++ {
		buf.WriteString(strconv.FormatUint(i, 10) + "\r")
	}

	buf.WriteString(testContractPassword + "\r")
	require.NoError(t, generateAlphabetCreds(generateAlphabetCmd, nil))

	for i := uint64(0); i < size; i++ {
		p := filepath.Join(walletDir, innerring.GlagoliticLetter(i).String()+".json")
		w, err := wallet.NewWalletFromFile(p)
		require.NoError(t, err, "wallet doesn't exist")
		require.Equal(t, 3, len(w.Accounts), "not all accounts were created")
		for _, a := range w.Accounts {
			err := a.Decrypt(strconv.FormatUint(i, 10), keys.NEP2ScryptParams())
			require.NoError(t, err, "can't decrypt account")
			switch a.Label {
			case consensusAccountName:
				require.Equal(t, smartcontract.GetDefaultHonestNodeCount(size), len(a.Contract.Parameters))
			case committeeAccountName:
				require.Equal(t, smartcontract.GetMajorityHonestNodeCount(size), len(a.Contract.Parameters))
			default:
				require.Equal(t, singleAccountName, a.Label)
			}
		}
	}

	t.Run("check contract group wallet", func(t *testing.T) {
		p := filepath.Join(walletDir, contractWalletFilename)
		w, err := wallet.NewWalletFromFile(p)
		require.NoError(t, err, "contract wallet doesn't exist")
		require.Equal(t, 1, len(w.Accounts), "contract wallet must have 1 accout")
		require.NoError(t, w.Accounts[0].Decrypt(testContractPassword, keys.NEP2ScryptParams()))
	})
}

func setupTestTerminal(t *testing.T) *bytes.Buffer {
	in := bytes.NewBuffer(nil)
	input.Terminal = term.NewTerminal(input.ReadWriter{
		Reader: in,
		Writer: io.Discard,
	}, "")

	t.Cleanup(func() { input.Terminal = nil })

	return in
}
