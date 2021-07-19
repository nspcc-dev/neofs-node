package morph

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-adm/internal/modules/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type initializeContext struct {
	Client *client.Client
	// CommitteeAcc is used for retrieving committee address and verification script.
	CommitteeAcc *wallet.Account
	// ConsensusAcc is used for retrieving committee address and verification script.
	ConsensusAcc *wallet.Account
	Wallets      []*wallet.Wallet
	Hashes       []util.Uint256
	WaitDuration time.Duration
	PollInterval time.Duration
}

func initializeSideChainCmd(cmd *cobra.Command, args []string) error {
	// contract path is not part of the config
	contractsPath, err := cmd.Flags().GetString(contractsInitFlag)
	if err != nil {
		return err
	}

	initCtx, err := newInitializeContext(viper.GetViper())
	if err != nil {
		return fmt.Errorf("initialization error: %w", err)
	}

	// 1. Transfer funds to committee accounts.
	cmd.Println("Stage 1: transfer GAS to alphabet nodes.")
	if err := initCtx.transferFunds(); err != nil {
		return err
	}

	// TODO 2. Setup notary and alphabet nodes in designate contract.
	// TODO 3. Deploy NNS contract with ID=0.
	// TODO 4. Deploy NeoFS contracts.
	// TODO 5. Setup NeoFS contracts addresses in NNS.
	// TODO 6. Register candidates and call alphabet.Vote.

	cmd.Println("endpoint:", viper.GetString(endpointFlag))
	cmd.Println("alphabet-wallets:", viper.GetString(alphabetWalletsFlag))
	cmd.Println("contracts:", contractsPath)
	cmd.Println("epoch-duration:", viper.GetUint(epochDurationInitFlag))
	cmd.Println("max-object-size:", viper.GetUint(maxObjectSizeInitFlag))

	return nil
}

func newInitializeContext(v *viper.Viper) (*initializeContext, error) {
	walletDir := v.GetString(alphabetWalletsFlag)
	wallets, err := openAlphabetWallets(walletDir)
	if err != nil {
		return nil, err
	}

	c, err := getN3Client(v)
	if err != nil {
		return nil, fmt.Errorf("can't create N3 client: %w", err)
	}

	committeeAcc, err := getWalletAccount(wallets[0], committeeAccountName)
	if err != nil {
		return nil, fmt.Errorf("can't find committee account: %w", err)
	}

	consensusAcc, err := getWalletAccount(wallets[0], consensusAccountName)
	if err != nil {
		return nil, fmt.Errorf("can't find consensus account: %w", err)
	}

	initCtx := &initializeContext{
		Client:       c,
		ConsensusAcc: consensusAcc,
		CommitteeAcc: committeeAcc,
		Wallets:      wallets,
		WaitDuration: time.Second * 30,
		PollInterval: time.Second,
	}

	return initCtx, nil
}

func openAlphabetWallets(walletDir string) ([]*wallet.Wallet, error) {
	walletFiles, err := ioutil.ReadDir(walletDir)
	if err != nil {
		return nil, fmt.Errorf("can't read alphabet wallets dir: %w", err)
	}

	size := len(walletFiles)
	if size == 0 {
		return nil, errors.New("alphabet wallets dir is empty (run `generate-alphabet` command first)")
	}

	wallets := make([]*wallet.Wallet, size)
	for i := 0; i < size; i++ {
		p := path.Join(walletDir, innerring.GlagoliticLetter(i).String()+".json")
		w, err := wallet.NewWalletFromFile(p)
		if err != nil {
			return nil, fmt.Errorf("can't open wallet: %w", err)
		}

		password, err := config.AlphabetPassword(viper.GetViper(), i)
		if err != nil {
			return nil, fmt.Errorf("can't fetch password: %w", err)
		}

		for i := range w.Accounts {
			if err := w.Accounts[i].Decrypt(password, keys.NEP2ScryptParams()); err != nil {
				return nil, fmt.Errorf("can't unlock wallet: %w", err)
			}
		}

		wallets[i] = w
	}

	return wallets, nil
}

func (c *initializeContext) awaitTx() error {
	tick := time.NewTicker(c.PollInterval)
	defer tick.Stop()

	timer := time.NewTimer(c.WaitDuration)
	defer timer.Stop()

	at := trigger.Application

loop:
	for i := range c.Hashes {
		for {
			select {
			case <-tick.C:
				_, err := c.Client.GetApplicationLog(c.Hashes[i], &at)
				if err == nil {
					continue loop
				}
			case <-timer.C:
				return errors.New("timeout while waiting for transaction to persist")
			}
		}
	}

	return nil
}

func getWalletAccount(w *wallet.Wallet, typ string) (*wallet.Account, error) {
	for i := range w.Accounts {
		if w.Accounts[i].Label == typ {
			return w.Accounts[i], nil
		}
	}
	return nil, fmt.Errorf("account for '%s' not found", typ)
}
