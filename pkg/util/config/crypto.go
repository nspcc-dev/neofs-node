package config

import (
	"errors"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
)

// LoadAccount loads NEP-6 load, unlocks and returns the provided account.
func LoadAccount(path, addr, password string) (*wallet.Account, error) {
	w, err := wallet.NewWalletFromFile(path)
	if err != nil {
		return nil, err
	}

	var h util.Uint160
	if addr == "" {
		h = w.GetChangeAddress()
		if h.Equals(util.Uint160{}) {
			return nil, errors.New("can't find a suitable account in the wallet")
		}
	} else {
		h, err = address.StringToUint160(addr)
		if err != nil {
			return nil, err
		}
	}

	acc := w.GetAccount(h)
	if acc == nil {
		return nil, errors.New("account is missing")
	}

	if err := acc.Decrypt(password, w.Scrypt); err != nil {
		return nil, err
	}

	return acc, nil
}
