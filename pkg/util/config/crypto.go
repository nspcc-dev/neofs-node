package config

import (
	"errors"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
)

// LoadAccount loads NEP-6 load, unlocks and returns provided account.
func LoadAccount(path, addr, password string) (*wallet.Account, error) {
	w, err := wallet.NewWalletFromFile(path)
	if err != nil {
		return nil, err
	}

	defer w.Close()

	h, err := address.StringToUint160(addr)
	if err != nil {
		return nil, err
	}

	acc := w.GetAccount(h)
	if acc == nil {
		return nil, errors.New("account is missing")
	}

	if err := acc.Decrypt(password, keys.NEP2ScryptParams()); err != nil {
		return nil, err
	}

	return acc, nil
}
