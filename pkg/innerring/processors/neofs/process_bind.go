package neofs

import (
	"crypto/elliptic"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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
			zap.Error(err),
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
	scriptHash := e.User()

	u160, err := util.Uint160DecodeBytesBE(scriptHash)
	if err != nil {
		np.log.Error("could not decode script hash from bytes",
			zap.Error(err),
		)

		return
	}

	id := user.NewFromScriptHash(u160)

	prm := neofsid.CommonBindPrm{}
	prm.SetOwnerID(id[:])
	prm.SetKeys(e.Keys())
	prm.SetHash(e.bindCommon.TxHash())

	var typ string
	if e.bind {
		typ = "bind"
		err = np.neofsIDClient.AddKeys(prm)
	} else {
		typ = "unbind"
		err = np.neofsIDClient.RemoveKeys(prm)
	}

	if err != nil {
		np.log.Error(fmt.Sprintf("could not approve %s", typ),
			zap.Error(err))
	}
}
