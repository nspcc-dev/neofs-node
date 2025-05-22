package fschain

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	contractsPath = "../../../../../../neofs-contract/neofs-contract-v0.16.0.tar.gz"
	protoFileName = "proto.yml"
)

func TestInitialize(t *testing.T) {
	// This test needs neofs-contract tarball, so it is skipped by default.
	// It is here for performing local testing after the changes.
	t.Skip()

	t.Run("4 nodes", func(t *testing.T) {
		testInitialize(t, 4)
	})
	t.Run("7 nodes", func(t *testing.T) {
		testInitialize(t, 7)
	})
}

func testInitialize(t *testing.T, committeeSize int) {
	testdataDir := t.TempDir()
	v := viper.GetViper()

	generateTestData(t, testdataDir, committeeSize)
	v.Set(protoConfigPath, filepath.Join(testdataDir, protoFileName))

	// Set to the path or remove the next statement to download from the network.
	require.NoError(t, initCmd.Flags().Set(contractsInitFlag, contractsPath))
	v.Set(localDumpFlag, filepath.Join(testdataDir, "out"))
	v.Set(alphabetWalletsFlag, testdataDir)
	v.Set(epochDurationInitFlag, 1)
	v.Set(maxObjectSizeInitFlag, 1024)

	setTestCredentials(v, committeeSize)
	require.NoError(t, initializeFSChainCmd(initCmd, nil))

	t.Run("force-new-epoch", func(t *testing.T) {
		require.NoError(t, forceNewEpochCmd(forceNewEpoch, nil))
	})
	t.Run("set-config", func(t *testing.T) {
		require.NoError(t, setConfigCmd(setConfig, []string{"HomomorphicHashingDisabled=true"}))
	})
	t.Run("set-policy", func(t *testing.T) {
		require.NoError(t, setPolicyCmd(setPolicy, []string{"ExecFeeFactor=1"}))
	})
	t.Run("remove-node", func(t *testing.T) {
		pk, err := keys.NewPrivateKey()
		require.NoError(t, err)

		pub := pk.PublicKey().StringCompressed()
		require.NoError(t, removeNodesCmd(removeNodes, []string{pub}))
	})
}

func generateTestData(t *testing.T, dir string, size int) {
	v := viper.GetViper()
	v.Set(alphabetWalletsFlag, dir)

	sizeStr := strconv.FormatUint(uint64(size), 10)
	require.NoError(t, generateAlphabetCmd.Flags().Set(alphabetSizeFlag, sizeStr))

	setTestCredentials(v, size)
	require.NoError(t, generateAlphabetCreds(generateAlphabetCmd, nil))

	var pubs []string
	for i := range size {
		p := filepath.Join(dir, client.NNSAlphabetContractName(i)+".json")
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
	cfg.ProtocolConfiguration.ValidatorsCount = uint32(size)
	cfg.ProtocolConfiguration.TimePerBlock = time.Second
	cfg.ProtocolConfiguration.StandbyCommittee = pubs // sorted by alphabet number
	cfg.ProtocolConfiguration.P2PSigExtensions = true
	cfg.ProtocolConfiguration.VerifyTransactions = true
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)

	protoPath := filepath.Join(dir, protoFileName)
	require.NoError(t, os.WriteFile(protoPath, data, os.ModePerm))
}

func setTestCredentials(v *viper.Viper, size int) {
	for i := range size {
		v.Set("credentials."+client.NNSAlphabetContractName(i), strconv.FormatUint(uint64(i), 10))
	}
}
