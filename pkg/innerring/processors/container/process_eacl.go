package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"go.uber.org/zap"
)

func (cp *Processor) processSetEACL(e container.SetEACL) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore set EACL")
		return
	}

	err := cp.checkSetEACL(e)
	if err != nil {
		cp.log.Error("set EACL check failed",
			zap.String("error", err.Error()),
		)

		return
	}

	cp.approveSetEACL(e)
}

func (cp *Processor) checkSetEACL(e container.SetEACL) error {
	return nil
}

func (cp *Processor) approveSetEACL(e container.SetEACL) {
	err := cp.cnrClient.PutEACLBinary(e.Table(), e.PublicKey(), e.Signature())
	if err != nil {
		cp.log.Error("could not approve set EACL",
			zap.String("error", err.Error()),
		)
	}
}
