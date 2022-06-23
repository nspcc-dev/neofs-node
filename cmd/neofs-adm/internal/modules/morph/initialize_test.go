package morph

import (
	"encoding/hex"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestInitialize(t *testing.T) {
	// This test needs neofs-contract tarball, so it is skipped by default.
	// It is here for performing local testing after the changes.
	t.Skip()

	const contractsPath = "../../../../../../neofs-contract/neofs-contract-v0.15.2.tar.gz"
	const committeeSize = 7
	const validatorCount = committeeSize

	walletDir := newTempDir(t)
	buf := setupTestTerminal(t)
	v := viper.GetViper()

	testGenerateAlphabet(t, buf, v, committeeSize, walletDir)

	var pubs []string
	for i := 0; i < committeeSize; i++ {
		p := filepath.Join(walletDir, innerring.GlagoliticLetter(i).String()+".json")
		w, err := wallet.NewWalletFromFile(p)
		require.NoError(t, err, "wallet doesn't exist")
		for _, acc := range w.Accounts {
			if acc.Label == singleAccountName {
				pub, ok := vm.ParseSignatureContract(acc.Contract.Script)
				require.True(t, ok)
				pubs = append(pubs, hex.EncodeToString(pub))
				continue
			}
		}
	}

	cfg := config.Config{}
	cfg.ProtocolConfiguration.ValidatorsCount = validatorCount
	cfg.ProtocolConfiguration.SecondsPerBlock = 1
	cfg.ProtocolConfiguration.StandbyCommittee = pubs // sorted by glagolic letters
	cfg.ProtocolConfiguration.P2PSigExtensions = true
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)

	protoPath := filepath.Join(walletDir, "proto.yml")
	require.NoError(t, ioutil.WriteFile(protoPath, data, os.ModePerm))

	v.Set(protoConfigPath, protoPath)
	// Set to the path or remove the next statement to download from the network.
	require.NoError(t, initCmd.Flags().Set(contractsInitFlag, contractsPath))
	for i := 0; i < committeeSize; i++ {
		v.Set("credentials."+innerring.GlagoliticLetter(i).String(), strconv.FormatUint(uint64(i), 10))
	}
	v.Set("credentials.contract", testContractPassword)
	v.Set(localDumpFlag, filepath.Join(walletDir, "out"))
	v.Set(alphabetWalletsFlag, walletDir)
	v.Set(epochDurationInitFlag, 1)
	v.Set(maxObjectSizeInitFlag, 1024)
	require.NoError(t, initializeSideChainCmd(initCmd, nil))
}
