package innerring

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/deploy"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
)

type neoFSSidechain struct {
	client *client.Client

	netmapContractMtx sync.RWMutex
	netmapContract    *netmap.Client
}

func newNeoFSSidechain(sidechainClient *client.Client) *neoFSSidechain {
	return &neoFSSidechain{
		client: sidechainClient,
	}
}

func (x *neoFSSidechain) CurrentState() (deploy.NeoFSState, error) {
	var res deploy.NeoFSState
	var err error

	x.netmapContractMtx.RLock()
	netmapContract := x.netmapContract
	x.netmapContractMtx.RUnlock()

	if netmapContract == nil {
		x.netmapContractMtx.Lock()

		if x.netmapContract == nil {
			netmapContractAddress, err := x.client.NNSContractAddress(client.NNSNetmapContractName)
			if err != nil {
				x.netmapContractMtx.Unlock()
				return res, fmt.Errorf("resolve address of the '%s' contract in NNS: %w", client.NNSNetmapContractName, err)
			}

			x.netmapContract, err = netmap.NewFromMorph(x.client, netmapContractAddress, 0)
			if err != nil {
				x.netmapContractMtx.Unlock()
				return res, fmt.Errorf("create Netmap contract client: %w", err)
			}
		}

		netmapContract = x.netmapContract

		x.netmapContractMtx.Unlock()
	}

	res.CurrentEpoch, err = netmapContract.Epoch()
	if err != nil {
		return res, fmt.Errorf("get current epoch from Netmap contract: %w", err)
	}

	res.CurrentEpochBlock, err = netmapContract.LastEpochBlock()
	if err != nil {
		return res, fmt.Errorf("get last epoch block from Netmap contract: %w", err)
	}

	return res, nil
}

type sidechainKeyStorage struct {
	persistentStorage *state.PersistentStorage
}

func newSidechainKeyStorage(persistentStorage *state.PersistentStorage) *sidechainKeyStorage {
	return &sidechainKeyStorage{
		persistentStorage: persistentStorage,
	}
}

var committeeGroupKey = []byte("committeeGroupKey")

// GetPersistedPrivateKey reads persisted private key from the underlying
// storage. If key is missing, it's randomized and saved first.
func (x *sidechainKeyStorage) GetPersistedPrivateKey() (*keys.PrivateKey, error) {
	b, err := x.persistentStorage.Bytes(committeeGroupKey)
	if err != nil {
		return nil, fmt.Errorf("read persistent storage: %w", err)
	}

	const password = ""

	if b != nil {
		var wlt wallet.Wallet

		err = json.Unmarshal(b, &wlt)
		if err != nil {
			return nil, fmt.Errorf("decode persisted NEO wallet from JSON: %w", err)
		}

		if len(wlt.Accounts) != 1 {
			return nil, fmt.Errorf("unexpected number of accounts in the persisted NEO wallet: %d instead of 1", len(wlt.Accounts))
		}

		err = wlt.Accounts[0].Decrypt(password, keys.NEP2ScryptParams())
		if err != nil {
			return nil, fmt.Errorf("unlock 1st NEO account of the persisted NEO wallet: %w", err)
		}

		return wlt.Accounts[0].PrivateKey(), nil
	}

	acc, err := wallet.NewAccount()
	if err != nil {
		return nil, fmt.Errorf("generate random NEO account: %w", err)
	}

	scryptPrm := keys.NEP2ScryptParams()

	err = acc.Encrypt(password, scryptPrm)
	if err != nil {
		return nil, fmt.Errorf("protect NEO account with password: %w", err)
	}

	wlt := wallet.Wallet{
		Version:  "1.0", // copy-paste from wallet package
		Accounts: []*wallet.Account{acc},
		Scrypt:   scryptPrm,
	}

	jWallet, err := json.Marshal(wlt)
	if err != nil {
		return nil, fmt.Errorf("encode NEO wallet with randomized NEO account into JSON: %w", err)
	}

	err = x.persistentStorage.SetBytes(committeeGroupKey, jWallet)
	if err != nil {
		return nil, fmt.Errorf("save generated key in the persistent storage: %w", err)
	}

	return acc.PrivateKey(), nil
}
