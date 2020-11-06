package keyer

import (
	"crypto/elliptic"
	"encoding/hex"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

type (
	Dashboard struct {
		privKey     *keys.PrivateKey
		pubKey      *keys.PublicKey
		scriptHash3 util.Uint160

		multisigKeys keys.PublicKeys
	}
)

func (d Dashboard) PrettyPrint(uncompressed, useHex bool) {
	var (
		data []byte

		privKey, pubKey, wif, wallet3, sh3, shBE3, multiSigAddr string
	)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	if d.privKey != nil {
		privKey = d.privKey.String()

		wif = d.privKey.WIF()
		if useHex {
			wif = base58ToHex(wif)
		}
	}

	if d.pubKey != nil {
		if uncompressed {
			data = elliptic.Marshal(elliptic.P256(), d.pubKey.X, d.pubKey.Y)
		} else {
			data = d.pubKey.Bytes()
		}

		pubKey = hex.EncodeToString(data)
	}

	if !d.scriptHash3.Equals(util.Uint160{}) {
		sh3 = d.scriptHash3.StringLE()
		shBE3 = d.scriptHash3.StringBE()
		wallet3 = address.Uint160ToString(d.scriptHash3)

		if useHex {
			wallet3 = base58ToHex(wallet3)
		}
	}

	if len(d.multisigKeys) != 0 {
		u160, err := scriptHashFromMultikey(d.multisigKeys)
		if err != nil {
			panic("can't create multisig redeem script")
		}

		multiSigAddr = address.Uint160ToString(u160)
	}

	if privKey != "" {
		fmt.Fprintf(w, "PrivateKey\t%s\n", privKey)
	}

	if pubKey != "" {
		fmt.Fprintf(w, "PublicKey\t%s\n", pubKey)
	}

	if wif != "" {
		fmt.Fprintf(w, "WIF\t%s\n", wif)
	}

	if wallet3 != "" {
		fmt.Fprintf(w, "Wallet3.0\t%s\n", wallet3)
	}

	if sh3 != "" {
		fmt.Fprintf(w, "ScriptHash3.0\t%s\n", sh3)
	}

	if shBE3 != "" {
		fmt.Fprintf(w, "ScriptHash3.0BE\t%s\n", shBE3)
	}

	if multiSigAddr != "" {
		fmt.Fprintf(w, "MultiSigAddress\t%s\n", multiSigAddr)
	}

	w.Flush()
}

func base58ToHex(data string) string {
	val, err := base58.Decode(data)
	if err != nil {
		panic("produced incorrect base58 value")
	}

	return hex.EncodeToString(val)
}

func scriptHashFromMultikey(k keys.PublicKeys) (util.Uint160, error) {
	script, err := smartcontract.CreateDefaultMultiSigRedeemScript(k)
	if err != nil {
		return util.Uint160{}, fmt.Errorf("can't create multisig redeem script: %w", err)
	}

	return hash.Hash160(script), nil
}
