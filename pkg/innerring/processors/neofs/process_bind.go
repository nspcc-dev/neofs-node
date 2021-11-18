package neofs

import (
	"crypto/elliptic"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/util"
	neofsid "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"go.uber.org/zap"
)

type bindCommon interface {
	User() []byte
	Keys() [][]byte
	TxHash() util.Uint256
}

func (np *Processor) processBind(e bindCommon) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore bind")
		return
	}

	c := &bindCommonContext{
		bindCommon: e,
	}

	_, c.bind = e.(neofs.Bind)

	err := np.checkBindCommon(c)
	if err != nil {
		np.log.Error("invalid manage key event",
			zap.Bool("bind", c.bind),
			zap.String("error", err.Error()),
		)

		return
	}

	np.approveBindCommon(c)
}

type bindCommonContext struct {
	bindCommon

	bind bool

	scriptHash util.Uint160
}

func (np *Processor) checkBindCommon(e *bindCommonContext) error {
	var err error

	e.scriptHash, err = util.Uint160DecodeBytesBE(e.User())
	if err != nil {
		return err
	}

	curve := elliptic.P256()

	for _, key := range e.Keys() {
		_, err = keys.NewPublicKeyFromBytes(key, curve)
		if err != nil {
			return err
		}
	}

	return nil
}

func (np *Processor) approveBindCommon(e *bindCommonContext) {
	// calculate wallet address
	// TODO: implement some utilities in API Go lib to do it
	scriptHash := e.User()

	u160, err := util.Uint160DecodeBytesBE(scriptHash)
	if err != nil {
		np.log.Error("could not decode script hash from bytes",
			zap.String("error", err.Error()),
		)

		return
	}

	wallet, err := base58.Decode(address.Uint160ToString(u160))
	if err != nil {
		np.log.Error("could not decode wallet address",
			zap.String("error", err.Error()),
		)

		return
	}

	prm := neofsid.ManageKeysPrm{}

	prm.SetOwnerID(wallet)
	prm.SetKeys(e.Keys())
	prm.SetAdd(e.bind)
	prm.SetHash(e.bindCommon.TxHash())

	err = np.neofsIDClient.ManageKeys(prm)
	if err != nil {
		var typ string

		if e.bind {
			typ = "bind"
		} else {
			typ = "unbind"
		}

		np.log.Error(fmt.Sprintf("could not approve %s", typ),
			zap.String("error", err.Error()),
		)
	}
}
