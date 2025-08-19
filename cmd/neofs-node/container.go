package main

import (
	"bytes"
	"errors"

	containerCore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	containerService "github.com/nspcc-dev/neofs-node/pkg/services/container"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

func initContainerService(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	if c.containerCache != nil {
		subscribeToContainerCreation(c, func(id cid.ID, owner user.ID) {
			if owner.IsZero() {
				c.log.Debug("container creation event's receipt", zap.Stringer("id", id))
				// read owner of the created container in order to update the reading cache.
				cnr, err := c.containerCache.Get(id)
				if err == nil {
					c.containerListCache.update(cnr.Owner(), id, true)
				} else {
					// unlike removal, we expect successful receive of the container
					// after successful creation, so logging can be useful
					c.log.Error("read newly created container after the notification", zap.Stringer("id", id), zap.Error(err))
				}
				return
			}
			c.log.Info("caught container creation, updating cache...", zap.Stringer("id", id), zap.Stringer("owner", owner))
			c.containerListCache.update(owner, id, true)
			c.containerCache.handleCreation(id)
			c.log.Info("successfully updated cache for the created container", zap.Stringer("id", id))
		})

		subscribeToContainerRemoval(c, func(id cid.ID, owner user.ID) {
			if owner.IsZero() {
				c.log.Debug("container removal event's receipt", zap.Stringer("id", id))
				// read owner of the removed container in order to update the listing cache.
				// It's strange to read already removed container, but we can successfully hit
				// the cache.
				cnr, err := c.containerCache.Get(id)
				if err == nil {
					c.containerListCache.update(cnr.Owner(), id, false)
				}
				c.containerCache.handleRemoval(id)
				return
			}
			c.log.Info("caught container removal, updating cache...", zap.Stringer("id", id),
				zap.Stringer("owner", owner))

			c.containerListCache.update(owner, id, false)
			c.containerCache.handleRemoval(id)

			c.log.Info("successfully updated cache of the removed container",
				zap.Stringer("id", id))
		})
	}
	if c.eaclCache != nil {
		subscribeToContainerEACLChange(c, func(cnr cid.ID) {
			c.log.Info("caught container eACL change, updating cache...", zap.Stringer("id", cnr))

			c.eaclCache.InvalidateEACL(cnr)

			c.log.Info("successfully updated cache of the container eACL", zap.Stringer("id", cnr))
		})
	}

	cnrSrv := containerService.New(&c.key.PrivateKey, c.networkState, c.cli, (*containersInChain)(&c.basics), c.nCli)
	addNewEpochAsyncNotificationHandler(c, func(event.Event) {
		cnrSrv.ResetSessionTokenCheckCache()
	})

	for _, srv := range c.cfgGRPC.servers {
		protocontainer.RegisterContainerServiceServer(srv, cnrSrv)
	}
}

// addContainerNotificationHandler adds handler that will be executed synchronously.
func addContainerNotificationHandler(c *cfg, sTyp string, h event.Handler) {
	typ := event.TypeFromString(sTyp)

	if c.cfgContainer.subscribers == nil {
		c.cfgContainer.subscribers = make(map[event.Type][]event.Handler, 1)
	}

	c.cfgContainer.subscribers[typ] = append(c.cfgContainer.subscribers[typ], h)
}

// addContainerAsyncNotificationHandler adds handler that will be executed asynchronously via container workerPool.
func addContainerAsyncNotificationHandler(c *cfg, sTyp string, h event.Handler) {
	addContainerNotificationHandler(
		c,
		sTyp,
		event.WorkerPoolHandler(
			c.cfgContainer.workerPool,
			h,
			c.log,
		),
	)
}

// stores already registered parsers of the notification events thrown by Container contract.
// MUST NOT be used concurrently.
var mRegisteredParsersContainer = make(map[string]struct{})

// registers event parser by name once. MUST NOT be called concurrently.
func registerEventParserOnceContainer(c *cfg, name string, p event.NotificationParser) {
	if _, ok := mRegisteredParsersContainer[name]; !ok {
		setContainerNotificationParser(c, name, p)
		mRegisteredParsersContainer[name] = struct{}{}
	}
}

// subscribes to successful container creation. Provided handler is called asynchronously
// on corresponding routine pool. MUST NOT be called concurrently with itself and other
// similar functions. Owner may be zero.
func subscribeToContainerCreation(c *cfg, h func(id cid.ID, owner user.ID)) {
	const eventNameContainerCreated = "PutSuccess"
	registerEventParserOnceContainer(c, eventNameContainerCreated, containerEvent.ParsePutSuccess)
	addContainerAsyncNotificationHandler(c, eventNameContainerCreated, func(e event.Event) {
		h(e.(containerEvent.PutSuccess).ID, user.ID{})
	})
	const eventNameContainerCreatedV2 = "Created"
	registerEventParserOnceContainer(c, eventNameContainerCreatedV2, containerEvent.RestoreCreated)
	addContainerAsyncNotificationHandler(c, eventNameContainerCreatedV2, func(e event.Event) {
		created := e.(containerEvent.Created)
		h(created.ID, created.Owner)
	})
}

// like subscribeToContainerCreation but for removal. Owner may be zero.
func subscribeToContainerRemoval(c *cfg, h func(id cid.ID, owner user.ID)) {
	const eventNameContainerRemoved = "DeleteSuccess"
	registerEventParserOnceContainer(c, eventNameContainerRemoved, containerEvent.ParseDeleteSuccess)
	addContainerAsyncNotificationHandler(c, eventNameContainerRemoved, func(e event.Event) {
		h(e.(containerEvent.DeleteSuccess).ID, user.ID{})
	})
	const eventNameContainerRemovedV2 = "Removed"
	registerEventParserOnceContainer(c, eventNameContainerRemovedV2, containerEvent.RestoreRemoved)
	addContainerAsyncNotificationHandler(c, eventNameContainerRemovedV2, func(e event.Event) {
		removed := e.(containerEvent.Removed)
		h(removed.ID, removed.Owner)
	})
}

// like subscribeToContainerCreation but for eACL setting.
func subscribeToContainerEACLChange(c *cfg, h func(cnr cid.ID)) {
	const eventNameEACLChanged = "EACLChanged"
	registerEventParserOnceContainer(c, eventNameEACLChanged, containerEvent.RestoreEACLChanged)
	addContainerAsyncNotificationHandler(c, eventNameEACLChanged, func(e event.Event) {
		eACLChanged := e.(containerEvent.EACLChanged)
		h(eACLChanged.Container)
	})
}

func setContainerNotificationParser(c *cfg, sTyp string, p event.NotificationParser) {
	typ := event.TypeFromString(sTyp)

	if c.cfgContainer.parsers == nil {
		c.cfgContainer.parsers = make(map[event.Type]event.NotificationParser, 1)
	}

	c.cfgContainer.parsers[typ] = p
}

func (c *cfg) PublicKey() []byte {
	return nodeKeyFromNetmap(c)
}

func (c *cfg) IsLocalKey(key []byte) bool {
	return bytes.Equal(key, c.PublicKey())
}

func (c *cfg) IterateAddresses(f func(string) bool) {
	c.iterateNetworkAddresses(f)
}

func (c *cfg) NumberOfAddresses() int {
	return c.addressNum()
}

type containersInChain basics

func (x *containersInChain) Get(id cid.ID) (containerSDK.Container, error) {
	return x.cnrSrc.Get(id)
}

func (x *containersInChain) GetEACL(id cid.ID) (eacl.Table, error) {
	return x.eaclSrc.GetEACL(id)
}

func (x *containersInChain) List(id user.ID) ([]cid.ID, error) {
	return x.cnrLst.List(&id)
}

func (x *containersInChain) Put(cnr containerSDK.Container, pub, sig []byte, st *session.Container) (cid.ID, error) {
	data := cnr.Marshal()
	d := cnr.ReadDomain()

	var prm cntClient.PutPrm
	prm.SetContainer(data)
	prm.SetName(d.Name())
	prm.SetZone(d.Zone())
	prm.SetKey(pub)
	prm.SetSignature(sig)
	if st != nil {
		prm.SetToken(st.Marshal())
	}
	if v := cnr.Attribute("__NEOFS__METAINFO_CONSISTENCY"); v == "optimistic" || v == "strict" {
		prm.EnableMeta()
	}
	if err := x.cCli.Put(prm); err != nil {
		return cid.ID{}, err
	}

	return cid.NewFromMarshalledContainer(data), nil
}

func (x *containersInChain) Delete(id cid.ID, pub, sig []byte, st *session.Container) error {
	var prm cntClient.DeletePrm
	prm.SetCID(id[:])
	prm.SetSignature(sig)
	prm.SetKey(pub)
	prm.RequireAlphabetSignature()
	if st != nil {
		prm.SetToken(st.Marshal())
	}
	return x.cCli.Delete(prm)
}

func (x *containersInChain) PutEACL(eACL eacl.Table, pub, sig []byte, st *session.Container) error {
	var prm cntClient.PutEACLPrm
	prm.SetTable(eACL.Marshal())
	prm.SetKey(pub)
	prm.SetSignature(sig)
	prm.RequireAlphabetSignature()
	if st != nil {
		prm.SetToken(st.Marshal())
	}
	if err := x.cCli.PutEACL(prm); err != nil {
		return err
	}

	return nil
}

type containerPresenceChecker struct{ src containerCore.Source }

// Exists implements [meta.Containers].
func (x containerPresenceChecker) Exists(id cid.ID) (bool, error) {
	if _, err := x.src.Get(id); err != nil {
		if errors.Is(err, apistatus.ErrContainerNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
