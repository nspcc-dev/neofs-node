package mainchain

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	scContext "github.com/nspcc-dev/neo-go/pkg/smartcontract/context"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-contract/rpc/neofs"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/n3util"
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
	senderWalletFlag    = "sender-wallet"

	committeeAccountName = "committee"
)

var updateContractCommand = &cobra.Command{
	Use:   "update",
	Short: "Update contract in main chain Neo network",
	Long: `Update contract in main chain Neo network using alphabet wallets for multisig.
This command creates a transaction calling the 'update' method of the specified contract,
signs it with alphabet wallets, and sends to the Neo RPC node.

The transaction requires two signers:
1. Sender wallet (pays transaction fees) - must have sufficient GAS balance
2. Committee multisig (authorizes the contract update) - alphabet wallets

Example:
  neofs-adm mainchain update \
    --config ./wallet-config.yml \
    --alphabet-wallets ./alphabet-wallets \
    --sender-wallet ./sender-wallet.json \
    --rpc-endpoint http://main-chain.neofs.devenv:30333 \
    --contract-hash Nd7UQEh78WaVdfVaGKu6WaL8Hj9TGQ7Z3J \
    --nef neofs/contract.nef \
    --manifest neofs/manifest.json \
    --data nil`,
	PreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(alphabetWalletsFlag, cmd.Flags().Lookup(alphabetWalletsFlag))
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
		_ = viper.BindPFlag(senderWalletFlag, cmd.Flags().Lookup(senderWalletFlag))
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
	wallets, err := n3util.OpenAlphabetWallets(v, walletDir)
	if err != nil {
		return fmt.Errorf("can't open alphabet wallets: %w", err)
	}

	var senderAcc *wallet.Account

	senderWalletPath, _ := cmd.Flags().GetString(senderWalletFlag)
	if senderWalletPath != "" {
		senderWallet, err := n3util.OpenWallet(senderWalletPath, v)
		if err != nil {
			return fmt.Errorf("can't open sender wallet: %w", err)
		}
		senderDefaultAddress := senderWallet.GetChangeAddress()
		senderAcc = senderWallet.GetAccount(senderDefaultAddress)
	}

	c, err := n3util.GetN3Client(v)
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	alphabetAcc, err := n3util.GetWalletAccount(wallets[0], committeeAccountName)
	if err != nil {
		return fmt.Errorf("can't find committee account: %w", err)
	}

	// Create one or two signers:
	// 1. Sender wallet - pays fees if specified
	// 2. Alphabet multisig - authorizes contract call
	var (
		signers             = make([]actor.SignerAccount, 0, 2)
		alphabetSignerIndex = 0
	)
	if senderAcc != nil {
		signers = append(signers, actor.SignerAccount{
			Signer: transaction.Signer{
				Account: senderAcc.ScriptHash(),
				Scopes:  transaction.None,
			},
			Account: senderAcc,
		})
		alphabetSignerIndex = 1
	}
	signers = append(signers, actor.SignerAccount{
		Signer: transaction.Signer{
			Account: alphabetAcc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		},
		Account: alphabetAcc,
	})

	act, err := actor.New(c, signers)
	if err != nil {
		return fmt.Errorf("can't create actor: %w", err)
	}

	contract := neofs.New(act, contractHash)
	tx, err := contract.UpdateUnsigned(nefBytes, manifestBytes, data)
	if err != nil {
		return fmt.Errorf("can't create update script: %w", err)
	}

	networkMagic := act.GetNetwork()
	if senderAcc != nil {
		err = senderAcc.SignTx(networkMagic, tx)
		if err != nil {
			return fmt.Errorf("can't sign transaction with gas wallet: %w", err)
		}
	}

	if err := multiSignTransactionAt(tx, wallets, committeeAccountName, networkMagic, alphabetSignerIndex); err != nil {
		return fmt.Errorf("can't sign transaction: %w", err)
	}

	txHash, vub, err := act.Send(tx)
	_, err = act.WaitSuccess(context.TODO(), txHash, vub, err)
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	cmd.Printf("Transaction %s sent successfully!\n", txHash.StringLE())
	return nil
}

func multiSignTransactionAt(tx *transaction.Transaction, wallets []*wallet.Wallet, accType string, network netmode.Magic, signerIndex int) error {
	firstAcc, err := n3util.GetWalletAccount(wallets[0], accType)
	if err != nil {
		return fmt.Errorf("can't get first wallet account: %w", err)
	}

	scriptHash := firstAcc.Contract.ScriptHash()

	pc := scContext.NewParameterContext("", network, tx)

	for _, w := range wallets {
		acc, err := n3util.GetWalletAccount(w, accType)
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

	if signerIndex < len(tx.Scripts) {
		tx.Scripts[signerIndex] = *witness
	} else if signerIndex == len(tx.Scripts) {
		tx.Scripts = append(tx.Scripts, *witness)
	} else {
		return fmt.Errorf("signer index %d out of range for signing order", signerIndex)
	}

	return nil
}

func initUpdateContractCmd() {
	flags := updateContractCommand.Flags()
	flags.String(alphabetWalletsFlag, "", "Path to alphabet wallets dir")
	flags.StringP(endpointFlag, "r", "", "N3 RPC node endpoint")
	flags.String(senderWalletFlag, "", "Path to wallet that will pay transaction fees (must have GAS)")
	flags.String(contractHashFlag, "", "Contract hash (address or hex)")
	flags.String(nefFlag, "", "Path to NEF file")
	flags.String(manifestFlag, "", "Path to manifest file")
	flags.String(dataFlag, "", "Optional data parameter for update method (default: nil)")

	_ = updateContractCommand.MarkFlagRequired(contractHashFlag)
	_ = updateContractCommand.MarkFlagRequired(nefFlag)
	_ = updateContractCommand.MarkFlagRequired(manifestFlag)
}
