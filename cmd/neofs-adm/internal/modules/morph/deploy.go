package morph

import (
	"fmt"
	"os"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	contractPathFlag = "contract"
	updateFlag       = "update"
)

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Deploy additional smart-contracts",
	Long: `Deploy additional smart-contract which are not related to core.
All contracts are deployed by the committee, so access to the alphabet wallets is required.`,
	PreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
	},
	RunE: deployContractCmd,
}

func init() {
	ff := deployCmd.Flags()

	ff.String(alphabetWalletsFlag, "", "Path to alphabet wallets dir")
	_ = deployCmd.MarkFlagFilename(alphabetWalletsFlag)

	ff.StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	ff.String(contractPathFlag, "", "Path to the contract directory")
	_ = deployCmd.MarkFlagFilename(contractPathFlag)

	ff.Bool(updateFlag, false, "Update an existing contract")
}

func deployContractCmd(cmd *cobra.Command, _ []string) error {
	v := viper.GetViper()
	c, err := newInitializeContext(cmd, v)
	if err != nil {
		return fmt.Errorf("initialization error: %w", err)
	}
	defer c.close()

	ctrPath, _ := cmd.Flags().GetString(contractPathFlag)
	ctrName, err := probeContractName(ctrPath)
	if err != nil {
		return err
	}

	cs, err := readContract(ctrPath, ctrName)
	if err != nil {
		return err
	}

	nnsCs, err := c.Client.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("can't fetch NNS contract state: %w", err)
	}

	callHash := c.nativeHash(nativenames.Management)
	method := deployMethodName
	domain := ctrName + ".neofs"
	isUpdate, _ := cmd.Flags().GetBool(updateFlag)
	if isUpdate {
		cs.Hash, err = nnsResolveHash(c.Client, nnsCs.Hash, domain)
		if err != nil {
			return fmt.Errorf("can't fetch contract hash from NNS: %w", err)
		}
		callHash = cs.Hash
		method = updateMethodName
	} else {
		cs.Hash = state.CreateContractHash(
			c.CommitteeAcc.Contract.ScriptHash(),
			cs.NEF.Checksum,
			cs.Manifest.Name)
	}

	err = c.addManifestGroup(cs.Hash, cs)
	if err != nil {
		return fmt.Errorf("can't sign manifest group: %v", err)
	}

	params := getContractDeployParameters(cs, nil)

	w := io.NewBufBinWriter()
	emit.AppCall(w.BinWriter, callHash, method, callflag.All, params...)
	emit.Opcodes(w.BinWriter, opcode.DROP) // contract state on stack
	if !isUpdate {
		s, err := c.nnsRegisterDomainScript(nnsCs.Hash, cs.Hash, domain)
		if err != nil {
			return err
		}
		if len(s) != 0 {
			c.Command.Printf("NNS: Set %s -> %s\n", domain, cs.Hash.StringLE())
			w.WriteBytes(s)
		}
	}

	if w.Err != nil {
		panic(fmt.Errorf("BUG: can't create deployment script: %w", w.Err))
	}

	if err := c.sendCommitteeTx(w.Bytes(), -1, false); err != nil {
		return err
	}
	return c.awaitTx()
}

func probeContractName(ctrPath string) (string, error) {
	ds, err := os.ReadDir(ctrPath)
	if err != nil {
		return "", fmt.Errorf("can't read directory: %w", err)
	}

	var ctrName string
	for i := range ds {
		if strings.HasSuffix(ds[i].Name(), "_contract.nef") {
			ctrName = strings.TrimSuffix(ds[i].Name(), "_contract.nef")
			break
		}
	}

	if ctrName == "" {
		return "", fmt.Errorf("can't find any NEF files in %s", ctrPath)
	}
	return ctrName, nil
}
