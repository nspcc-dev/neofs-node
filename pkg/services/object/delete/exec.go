package deletesvc

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

type statusError struct {
	status int
	err    error
}

type execCtx struct {
	svc *Service

	ctx context.Context

	prm Prm

	statusError

	log *logger.Logger

	tombstone *objectSDK.Tombstone

	splitInfo *objectSDK.SplitInfo

	tombstoneObj *object.RawObject
}

const (
	statusUndefined int = iota
	statusOK
)

func (exec *execCtx) setLogger(l *logger.Logger) {
	exec.log = l.With(
		zap.String("request", "DELETE"),
		zap.Stringer("address", exec.address()),
		zap.Bool("local", exec.isLocal()),
		zap.Bool("with session", exec.prm.common.SessionToken() != nil),
		zap.Bool("with bearer", exec.prm.common.BearerToken() != nil),
	)
}

func (exec execCtx) context() context.Context {
	return exec.ctx
}

func (exec execCtx) isLocal() bool {
	return exec.prm.common.LocalOnly()
}

func (exec *execCtx) key() *ecdsa.PrivateKey {
	return exec.prm.common.PrivateKey()
}

func (exec *execCtx) address() *objectSDK.Address {
	return exec.prm.Address()
}

func (exec *execCtx) containerID() *container.ID {
	return exec.prm.Address().ContainerID()
}

func (exec *execCtx) commonParameters() *util.CommonPrm {
	return exec.prm.common
}

func (exec execCtx) callOptions() []client.CallOption {
	return exec.prm.common.RemoteCallOptions()
}

func (exec *execCtx) newAddress(id *objectSDK.ID) *objectSDK.Address {
	a := objectSDK.NewAddress()
	a.SetObjectID(id)
	a.SetContainerID(exec.containerID())

	return a
}

func (exec *execCtx) formSplitInfo() bool {
	var err error

	exec.splitInfo, err = exec.svc.header.splitInfo(exec)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not compose split info",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}

	return err == nil
}

func (exec *execCtx) collectMembers() (ok bool) {
	if exec.splitInfo == nil {
		exec.log.Debug("no split info, object is PHY")
		return true
	}

	if exec.splitInfo.Link() != nil {
		ok = exec.collectChildren()
	}

	if !ok && exec.splitInfo.LastPart() != nil {
		ok = exec.collectChain()
		if !ok {
			return
		}
	} // may be fail if neither right nor linking ID is set?

	return exec.supplementBySplitID()
}

func (exec *execCtx) collectChain() bool {
	var (
		err   error
		chain []*objectSDK.ID
	)

	exec.log.Debug("assembling chain...")

	for prev := exec.splitInfo.LastPart(); prev != nil; {
		chain = append(chain, prev)
		prev, err = exec.svc.header.previous(exec, prev)

		switch {
		default:
			exec.status = statusUndefined
			exec.err = err

			exec.log.Debug("could not get previous split element",
				zap.Stringer("id", prev),
				zap.String("error", err.Error()),
			)

			return false
		case err == nil:
			exec.status = statusOK
			exec.err = nil
		}
	}

	exec.addMembers(chain)

	return true
}

func (exec *execCtx) collectChildren() bool {
	exec.log.Debug("collecting children...")

	children, err := exec.svc.header.children(exec)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not collect object children",
			zap.String("error", err.Error()),
		)

		return false
	case err == nil:
		exec.status = statusOK
		exec.err = nil

		exec.addMembers(append(children, exec.splitInfo.Link()))

		return true
	}
}

func (exec *execCtx) supplementBySplitID() bool {
	exec.log.Debug("supplement by split ID")

	chain, err := exec.svc.searcher.splitMembers(exec)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not search for split chain members",
			zap.String("error", err.Error()),
		)

		return false
	case err == nil:
		exec.status = statusOK
		exec.err = nil

		exec.addMembers(chain)

		return true
	}
}

func (exec *execCtx) addMembers(incoming []*objectSDK.ID) {
	members := exec.tombstone.Members()

	for i := range members {
		for j := 0; j < len(incoming); j++ { // don't use range, slice mutates in body
			if members[i].Equal(incoming[j]) {
				incoming = append(incoming[:j], incoming[j+1:]...)
				j--
			}
		}
	}

	exec.tombstone.SetMembers(append(members, incoming...))
}

func (exec *execCtx) initTombstoneObject() bool {
	payload, err := exec.tombstone.Marshal()
	if err != nil {
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not marshal tombstone structure",
			zap.String("error", err.Error()),
		)

		return false
	}

	exec.tombstoneObj = object.NewRaw()
	exec.tombstoneObj.SetContainerID(exec.containerID())
	exec.tombstoneObj.SetOwnerID(exec.commonParameters().SessionToken().OwnerID())
	exec.tombstoneObj.SetType(objectSDK.TypeTombstone)
	exec.tombstoneObj.SetPayload(payload)

	return true
}

func (exec *execCtx) saveTombstone() bool {
	id, err := exec.svc.placer.put(exec, false)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not save the tombstone",
			zap.String("error", err.Error()),
		)

		return false
	case err == nil:
		exec.status = statusOK
		exec.err = nil

		exec.prm.TombstoneAddressTarget().
			SetAddress(exec.newAddress(id))
	}

	return true
}

func (exec *execCtx) broadcastTombstone() bool {
	_, err := exec.svc.placer.put(exec, true)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not save the tombstone",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}

	return err == nil
}
