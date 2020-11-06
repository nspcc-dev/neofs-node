package keyer

import (
	"crypto/elliptic"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

const (
	NeoPrivateKeySize = 32

	scriptHashSize            = 20
	addressSize               = 25
	publicKeyCompressedSize   = 33
	wifSize                   = 38
	publicKeyUncompressedSize = 65
)

var errInputType = errors.New("unknown input type")

func (d *Dashboard) ParseString(data string) error {
	// just in case remove 0x prefixes if there are some
	data = strings.TrimPrefix(data, "0x")
	data = strings.TrimPrefix(data, "0X")

	var (
		rawData []byte
		err     error
	)

	// data could be encoded in base58 or hex formats, try both
	rawData, err = base58.Decode(data)
	if err != nil {
		rawData, err = hex.DecodeString(data)
		if err != nil {
			return fmt.Errorf("data is not hex or base58 encoded: %w", err)
		}
	}

	return d.ParseBinary(rawData)
}

func (d *Dashboard) ParseBinary(data []byte) error {
	var err error

	switch len(data) {
	case NeoPrivateKeySize:
		d.privKey, err = keys.NewPrivateKeyFromBytes(data)
		if err != nil {
			return fmt.Errorf("can't parse private key: %w", err)
		}
	case wifSize:
		d.privKey, err = keys.NewPrivateKeyFromWIF(base58.Encode(data))
		if err != nil {
			return fmt.Errorf("can't parse WIF: %w", err)
		}
	case publicKeyCompressedSize, publicKeyUncompressedSize:
		d.pubKey, err = keys.NewPublicKeyFromBytes(data, elliptic.P256())
		if err != nil {
			return fmt.Errorf("can't parse public key: %w", err)
		}
	case addressSize:
		d.scriptHash3, err = address.StringToUint160(base58.Encode(data))
		if err != nil {
			return fmt.Errorf("can't parse address: %w", err)
		}
	case scriptHashSize:
		sc, err := util.Uint160DecodeBytesLE(data)
		if err != nil {
			return fmt.Errorf("can't parse script hash: %w", err)
		}

		d.scriptHash3 = sc
	default:
		return errInputType
	}

	d.fill()

	return nil
}

func (d *Dashboard) ParseMultiSig(data []string) error {
	d.multisigKeys = make(keys.PublicKeys, 0, len(data))

	for i := range data {
		data, err := hex.DecodeString(data[i])
		if err != nil {
			return fmt.Errorf("pass only hex encoded public keys: %w", err)
		}

		key, err := keys.NewPublicKeyFromBytes(data, elliptic.P256())
		if err != nil {
			return fmt.Errorf("pass only hex encoded public keys: %w", err)
		}

		d.multisigKeys = append(d.multisigKeys, key)
	}

	return nil
}

func (d *Dashboard) fill() {
	if d.privKey != nil {
		d.pubKey = d.privKey.PublicKey()
	}

	if d.pubKey != nil {
		d.scriptHash3 = d.pubKey.GetScriptHash()
	}
}
