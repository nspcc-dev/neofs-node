package mainchain

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	io2 "github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	scContext "github.com/nspcc-dev/neo-go/pkg/smartcontract/context"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/fschain"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	alphabetWalletsFlag = "alphabet-wallets"
	endpointFlag        = "rpc-endpoint"
	contractHashFlag    = "contract-hash"
	nefFlag             = "nef"
	manifestFlag        = "manifest"
	dataFlag            = "data"
	awaitFlag           = "await"
	gasWalletFlag       = "gas-wallet"

	committeeAccountName = "committee"

	maxAttemptsTxWait = 20
)

var updateContractCommand = &cobra.Command{
	Use:   "update-contract",
	Short: "Update contract in main chain Neo network",
	Long: `Update contract in main chain Neo network using alphabet wallets for multisig.
This command creates a transaction calling the 'update' method of the specified contract,
signs it with alphabet wallets, and sends to the Neo RPC node.

The transaction requires two signers:
1. Gas wallet (pays transaction fees) - must have sufficient GAS balance
2. Committee multisig (authorizes the contract update) - alphabet wallets

Example:
  neofs-adm mainchain update-contract \
    --config ./wallet-config.yml \
    --alphabet-wallets ./alphabet-wallets \
    --gas-wallet ./gas-wallet.json \
    --rpc-endpoint http://main-chain.neofs.devenv:30333 \
    --contract-hash Nd7UQEh78WaVdfVaGKu6WaL8Hj9TGQ7Z3J \
    --nef neofs/contract.nef \
    --manifest neofs/manifest.json \
    --data nil \
    --await`,
	PreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		_ = viper.BindPFlag(gasWalletFlag, cmd.Flags().Lookup(gasWalletFlag))
	},
	RunE: updateContractCmd,
}

func updateContractCmd(cmd *cobra.Command, _ []string) error {
	contractHashStr, _ := cmd.Flags().GetString(contractHashFlag)
	if contractHashStr == "" {
		return errors.New("contract hash is required")
	}

	contractHash, err := util.Uint160DecodeStringLE(strings.TrimPrefix(contractHashStr, "0x"))
	if err != nil {
		contractHash, err = address.StringToUint160(contractHashStr)
		if err != nil {
			return fmt.Errorf("invalid contract hash: %w", err)
		}
	}

	nefFile, _ := cmd.Flags().GetString(nefFlag)
	if nefFile == "" {
		return errors.New("NEF file is required")
	}

	manifestFile, _ := cmd.Flags().GetString(manifestFlag)
	if manifestFile == "" {
		return errors.New("manifest file is required")
	}

	dataStr, _ := cmd.Flags().GetString(dataFlag)
	var data any
	if dataStr != "" && dataStr != "nil" {
		data = dataStr
	}

	nefBytes, err := os.ReadFile(nefFile)
	if err != nil {
		return fmt.Errorf("can't read NEF file: %w", err)
	}

	manifestBytes, err := os.ReadFile(manifestFile)
	if err != nil {
		return fmt.Errorf("can't read manifest file: %w", err)
	}

	v := viper.GetViper()
	walletDir := config.ResolveHomePath(viper.GetString(alphabetWalletsFlag))
	wallets, err := openAlphabetWallets(v, walletDir)
	if err != nil {
		return fmt.Errorf("can't open alphabet wallets: %w", err)
	}

	gasWalletPath, _ := cmd.Flags().GetString(gasWalletFlag)
	if gasWalletPath == "" {
		return errors.New("gas wallet is required (use --gas-wallet flag)")
	}
	gasWallet, err := openWallet(gasWalletPath, v)
	if err != nil {
		return fmt.Errorf("can't open gas wallet: %w", err)
	}
	gasAcc, err := fschain.GetWalletAccount(gasWallet, "single")
	if err != nil {
		return fmt.Errorf("can't find gas wallet account: %w", err)
	}

	c, err := fschain.GetN3Client(v)
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	committeeAcc, err := fschain.GetWalletAccount(wallets[0], committeeAccountName)
	if err != nil {
		return fmt.Errorf("can't find committee account: %w", err)
	}

	// Create two signers:
	// 1. Gas wallet - pays fees (FeePayer with CalledByEntry)
	// 2. Committee multisig - authorizes contract call (CalledByEntry)
	signers := []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: gasAcc.Contract.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: gasAcc,
		},
		{
			Signer: transaction.Signer{
				Account: committeeAcc.Contract.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: committeeAcc,
		},
	}

	act, err := actor.New(c, signers)
	if err != nil {
		return fmt.Errorf("can't create actor: %w", err)
	}

	w := io2.NewBufBinWriter()
	emit.AppCall(w.BinWriter, contractHash, "update", callflag.All, nefBytes, manifestBytes, data)
	script := w.Bytes()

	tx, err := act.MakeUnsignedRun(script, nil)
	if err != nil {
		return fmt.Errorf("can't create unsigned transaction: %w", err)
	}

	networkMagic := act.GetNetwork()

	gasSignature := gasAcc.PrivateKey().SignHashable(uint32(networkMagic), tx)
	gasWitness := transaction.Witness{
		InvocationScript:   append([]byte{byte(opcode.PUSHDATA1), 64}, gasSignature...),
		VerificationScript: gasAcc.Contract.Script,
	}
	gasScriptHash := gasAcc.Contract.ScriptHash()
	for i := range tx.Signers {
		if tx.Signers[i].Account == gasScriptHash {
			tx.Scripts[i] = gasWitness
			break
		}
	}

	if err := multiSignTransaction(tx, wallets, committeeAccountName, networkMagic); err != nil {
		return fmt.Errorf("can't sign transaction: %w", err)
	}

	txHash, err := c.SendRawTransaction(tx)
	if err != nil {
		return fmt.Errorf("can't send transaction: %w", err)
	}

	cmd.Printf("Transaction sent successfully!\n")
	cmd.Printf("Transaction hash: %s\n", txHash.StringLE())

	await, _ := cmd.Flags().GetBool(awaitFlag)
	if await {
		cmd.Println("Waiting for transaction to be included in block...")
		if err := waitForTxMainChain(c, txHash, tx.ValidUntilBlock); err != nil {
			return fmt.Errorf("transaction failed: %w", err)
		}
		cmd.Println("Transaction confirmed")
	}

	return nil
}

func multiSignTransaction(tx *transaction.Transaction, wallets []*wallet.Wallet, accType string, network netmode.Magic) error {
	firstAcc, err := fschain.GetWalletAccount(wallets[0], accType)
	if err != nil {
		return fmt.Errorf("can't get first wallet account: %w", err)
	}

	scriptHash := firstAcc.Contract.ScriptHash()

	pc := scContext.NewParameterContext("", network, tx)

	for _, w := range wallets {
		acc, err := fschain.GetWalletAccount(w, accType)
		if err != nil {
			continue // Skip wallets that don't have this account
		}

		priv := acc.PrivateKey()
		sign := priv.SignHashable(uint32(network), tx)

		if err := pc.AddSignature(scriptHash, acc.Contract, priv.PublicKey(), sign); err != nil {
			return fmt.Errorf("can't add signature: %w", err)
		}

		if len(pc.Items[scriptHash].Signatures) == len(acc.Contract.Parameters) {
			break
		}
	}

	witness, err := pc.GetWitness(scriptHash)
	if err != nil {
		return fmt.Errorf("incomplete signature: %w", err)
	}

	for i := range tx.Signers {
		if tx.Signers[i].Account == scriptHash {
			if i < len(tx.Scripts) {
				tx.Scripts[i] = *witness
			} else if i == len(tx.Scripts) {
				tx.Scripts = append(tx.Scripts, *witness)
			} else {
				panic("BUG: invalid signing order")
			}
			return nil
		}
	}

	return fmt.Errorf("%s account was not found among transaction signers", accType)
}

func waitForTxMainChain(c fschain.Client, hash util.Uint256, vub uint32) error {
	for range maxAttemptsTxWait {
		time.Sleep(time.Second)

		height, err := c.GetBlockCount()
		if err == nil && height > vub {
			return fmt.Errorf("transaction expired: current height %d > vub %d", height, vub)
		}

		appLog, err := c.GetApplicationLog(hash, nil)
		if err == nil && len(appLog.Executions) > 0 {
			if appLog.Executions[0].VMState.HasFlag(vmstate.Halt) {
				return nil
			}
			return fmt.Errorf("transaction failed with state: %s, exception: %s",
				appLog.Executions[0].VMState, appLog.Executions[0].FaultException)
		}
	}

	return errors.New("timeout waiting for transaction")
}

func openAlphabetWallets(v *viper.Viper, walletDir string) ([]*wallet.Wallet, error) {
	walletFiles, err := os.ReadDir(walletDir)
	if err != nil {
		return nil, fmt.Errorf("can't read alphabet wallets dir: %w", err)
	}

	var wallets []*wallet.Wallet

	for _, walletFile := range walletFiles {
		isJson := strings.HasSuffix(walletFile.Name(), ".json")
		if walletFile.IsDir() || !isJson {
			continue // Ignore garbage.
		}

		w, err := openWallet(filepath.Join(walletDir, walletFile.Name()), v)
		if err != nil {
			return nil, fmt.Errorf("can't open wallet %s: %w", walletFile.Name(), err)
		}

		wallets = append(wallets, w)
	}
	if len(wallets) == 0 {
		return nil, errors.New("alphabet wallets dir is empty (run `generate-alphabet` command first)")
	}

	return wallets, nil
}

func openWallet(path string, v *viper.Viper) (*wallet.Wallet, error) {
	path = config.ResolveHomePath(path)
	w, err := wallet.NewWalletFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("can't open gas wallet: %w", err)
	}
	walletName := filepath.Base(path)
	walletName = strings.TrimSuffix(walletName, ".json")
	password, err := config.GetPassword(v, walletName)
	if err != nil {
		return nil, fmt.Errorf("can't fetch password: %w", err)
	}
	for i := range w.Accounts {
		if err := w.Accounts[i].Decrypt(password, w.Scrypt); err != nil {
			return nil, fmt.Errorf("can't unlock wallet: %w", err)
		}
	}
	return w, nil
}

func initUpdateContractCmd() {
	flags := updateContractCommand.Flags()
	flags.String(alphabetWalletsFlag, "", "Path to alphabet wallets dir")
	flags.StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	flags.String(gasWalletFlag, "", "Path to wallet that will pay transaction fees (must have GAS)")
	flags.String(contractHashFlag, "", "Contract hash (address or hex)")
	flags.String(nefFlag, "", "Path to NEF file")
	flags.String(manifestFlag, "", "Path to manifest file")
	flags.String(dataFlag, "", "Optional data parameter for update method (default: nil)")
	flags.Bool(awaitFlag, false, "Wait for the transaction to be included in a block")

	_ = updateContractCommand.MarkFlagRequired(gasWalletFlag)
	_ = updateContractCommand.MarkFlagRequired(contractHashFlag)
	_ = updateContractCommand.MarkFlagRequired(nefFlag)
	_ = updateContractCommand.MarkFlagRequired(manifestFlag)
}
