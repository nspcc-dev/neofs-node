package container

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
)

func (cp *Processor) handleCreationRequest(ev event.Event) {
	err := cp.pool.Submit(func() { cp.processCreateContainerRequest(ev.(containerEvent.CreateContainerV2Request)) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handlePut(ev event.Event) {
	req, ok := ev.(containerEvent.CreateContainerRequest)
	if !ok {
		e := ev.(putEvent)
		req.MainTransaction = *e.NotaryRequest().MainTransaction
		req.Container = e.Container()
		req.InvocationScript = e.Signature()
		req.VerificationScript = e.PublicKey()
		req.SessionToken = e.SessionToken()
		if n, ok := e.(interface {
			Name() string
			Zone() string
		}); ok {
			req.DomainName = n.Name()
			req.DomainZone = n.Zone()
		}
	}

	id := sha256.Sum256(req.Container)
	cp.log.Info("notification",
		zap.String("type", "container put"),
		zap.String("id", base58.Encode(id[:])))

	// send an event to the worker pool

	err := cp.pool.Submit(func() { cp.processContainerPut(req, id) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleDelete(ev event.Event) {
	req, ok := ev.(containerEvent.RemoveContainerRequest)
	if !ok {
		e := ev.(containerEvent.Delete)
		req.MainTransaction = *e.NotaryRequest().MainTransaction
		req.ID = e.ContainerID()
		req.InvocationScript = e.Signature()
		req.VerificationScript = nil
		req.SessionToken = e.SessionToken()
	} else if req.VerificationScript == nil {
		req.VerificationScript = []byte{} // to differ with 'delete' having no such parameter
	}
	cp.log.Info("notification",
		zap.String("type", "container delete"),
		zap.String("id", base58.Encode(req.ID)))

	// send an event to the worker pool

	err := cp.pool.Submit(func() { cp.processContainerDelete(req) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleSetEACL(ev event.Event) {
	req, ok := ev.(containerEvent.PutContainerEACLRequest)
	if !ok {
		e := ev.(containerEvent.SetEACL)
		req.MainTransaction = *e.NotaryRequest().MainTransaction
		req.EACL = e.Table()
		req.InvocationScript = e.Signature()
		req.VerificationScript = e.PublicKey()
		req.SessionToken = e.SessionToken()
	}

	cp.log.Info("notification",
		zap.String("type", "set EACL"),
	)

	// send an event to the worker pool

	err := cp.pool.Submit(func() {
		cp.processPutEACLRequest(req)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleAnnounceLoad(ev event.Event) {
	e := ev.(containerEvent.Report)

	cp.log.Info("notification",
		zap.String("type", "announce load"),
		zap.Stringer("cid", cid.ID(e.CID)),
		zap.Int64("storage size", e.StorageSize),
		zap.Int64("objects number", e.ObjectsNumber),
		zap.String("reporter", hex.EncodeToString(e.NodeKey)),
	)

	// send an event to the worker pool

	err := cp.pool.Submit(func() {
		cp.processAnnounceLoad(e)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		cp.log.Warn("container processor worker pool drained",
			zap.Int("capacity", cp.pool.Cap()))
	}
}

func (cp *Processor) handleObjectPut(ev event.Event) {
	e := ev.(containerEvent.ObjectPut)

	cp.log.Debug("notification",
		zap.String("type", "object put"),
		zap.Stringer("cID", e.ContainerID()),
		zap.Stringer("oID", e.ObjectID()),
	)

	if !cp.alphabetState.IsAlphabet() {
		cp.log.Debug("non alphabet mode, ignore object put")
		return
	}

	err := cp.objectPool.Submit(func() {
		nr := e.NotaryRequest()
		err := cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction, false)
		if err != nil {
			cp.log.Error("could not approve object put",
				zap.Stringer("cID", e.ContainerID()),
				zap.Stringer("oID", e.ObjectID()),
				zap.Error(err),
			)
		}
	})
	if err != nil {
		cp.log.Warn("object pool submission failed", zap.Error(err))
	}
}
