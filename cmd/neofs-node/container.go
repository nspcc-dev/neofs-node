package main

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	containerCore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	balanceEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	containerService "github.com/nspcc-dev/neofs-node/pkg/services/container"
	timer "github.com/nspcc-dev/neofs-node/pkg/timers"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
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

	initSizeLoadReports(c)

	cnrSrv := containerService.New(&c.key.PrivateKey, c.networkState, c.cli, (*containersInChain)(&c.basics), c.nCli)
	addNewEpochAsyncNotificationHandler(c, func(event.Event) {
		cnrSrv.ResetSessionTokenCheckCache()
	})

	for _, srv := range c.cfgGRPC.servers {
		protocontainer.RegisterContainerServiceServer(srv, cnrSrv)
	}
}

func initSizeLoadReports(c *cfg) {
	l := c.log.With(zap.String("component", "containerReports"))

	// hardcoded in neofs-contracts as of 957152c7da6eb0a20745cfa1c4513b3c3512db6e
	const maxReportsPerEpoch = 3

	nm, err := c.nCli.NetMap()
	if err != nil {
		fatalOnErr(fmt.Errorf("failed to get netmap to initialize container service: %w", err))
	}
	durSec, err := c.nCli.EpochDuration()
	if err != nil {
		fatalOnErr(fmt.Errorf("failed to get epoch duration to initialize container service: %w", err))
	}
	dur := time.Duration(durSec) * time.Second

	var nmLen = len(nm.Nodes())
	indexInNM := slices.IndexFunc(nm.Nodes(), func(node netmapsdk.NodeInfo) bool {
		return bytes.Equal(node.PublicKey(), c.binPublicKey)
	})
	if indexInNM == -1 {
		indexInNM = nmLen
	}

	var stepsInEpoch uint32
	if nmLen != 0 {
		// likely there is no need to report less often than once per second
		reportStep := min(time.Second, dur/maxReportsPerEpoch/time.Duration(nmLen))
		stepsInEpoch = uint32(dur / reportStep)
	}

	var (
		ticks      timer.EpochTicks
		reportTick = reportHandler(c, l)
	)
	for i := range maxReportsPerEpoch {
		var mul, div uint32
		if stepsInEpoch == 0 {
			mul = uint32(i)
			div = maxReportsPerEpoch
		} else {
			mul = uint32(i)*(stepsInEpoch/maxReportsPerEpoch) + uint32(indexInNM)
			div = stepsInEpoch
		}

		l.Debug("add space load reporter", zap.Uint32("multiplicator", mul), zap.Uint32("divisor", div))

		ticks.DeltaTicks = append(ticks.DeltaTicks, timer.SubEpochTick{
			Tick:     reportTick,
			EpochMul: mul,
			EpochDiv: div,
		})
	}

	c.cfgMorph.epochTimers = timer.NewTimers(ticks)
	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		txHash := ev.(netmap.NewEpoch).TxHash()
		txHeight, err := c.nCli.Morph().TxHeight(txHash)
		if err != nil {
			l.Warn("can't get transaction height", zap.String("hash", txHash.StringLE()), zap.Error(err))
			return
		}
		epochDuration, err := c.nCli.EpochDuration()
		if err != nil {
			l.Warn("failed to get the epoch duration to reset epoch timer", zap.Error(err))
			return
		}
		lastTick := txHeight
		if lastTick == 0 {
			lastTick, err = c.nCli.LastEpochBlock()
			if err != nil {
				l.Warn("failed to get the last epoch block number to reset epoch timer", zap.Error(err))
				return
			}
		}
		lastTickH, err := c.cli.GetBlockHeader(lastTick)
		if err != nil {
			l.Warn("failed to get the last epoch block to reset epoch timer",
				zap.Uint32("lastTickHeight", lastTick), zap.Error(err))

			return
		}

		const msInS = 1000
		c.cfgMorph.epochTimers.Reset(lastTickH.Timestamp, epochDuration*msInS)
	})
}

func reportHandler(c *cfg, logger *zap.Logger) timer.Tick {
	type report struct {
		size, objsNum uint64
	}
	var (
		m     sync.RWMutex
		cache = make(map[cid.ID]report)
	)

	return func() {
		epoch := c.CurrentEpoch()
		st := c.cfgObject.cfgLocalStorage.localStorage
		l := logger.With(zap.Uint64("epoch", epoch))

		l.Debug("sending container reports to contract...")

		idList, err := st.ListContainers()
		if err != nil {
			l.Warn("engine's list containers failure", zap.Error(err))
			return
		}

		if len(idList) == 0 {
			l.Debug("no containers found in storage to report")
			return
		}

		networkMap, err := c.netMapSource.GetNetMapByEpoch(epoch)
		if err != nil {
			l.Warn("unable to fetch network map", zap.Error(err))
			return
		}

		var successes int
		for _, cnr := range idList {
			size, objsNum, err := st.ContainerInfo(cnr)
			if err != nil {
				l.Warn("container's stat fetching error", zap.Stringer("cid", cnr), zap.Error(err))
				return
			}

			m.RLock()
			reportedBefore, ok := cache[cnr]
			m.RUnlock()

			if ok && reportedBefore.size == size && reportedBefore.objsNum == objsNum {
				l.Debug("skip reporting disk load for the container, as values are the same",
					zap.Uint64("size", size), zap.Uint64("objsNum", objsNum), zap.Stringer("cid", cnr))

				continue
			}

			cont, err := c.cnrSrc.Get(cnr)
			if err != nil {
				l.Warn("unable to fetch container data", zap.Stringer("cid", cnr), zap.Error(err))
				continue
			}

			if !isContainerMine(cont, networkMap, c.binPublicKey) {
				l.Debug("got usage data for alien container, can't report", zap.Stringer("cid", cnr))
				continue
			}

			err = c.cCli.PutReport(cnr, size, objsNum, c.PublicKey())
			if err != nil {
				l.Warn("put report to contract error", zap.Stringer("cid", cnr), zap.Error(err))
				continue
			}

			m.Lock()
			reportedBefore.size = size
			reportedBefore.objsNum = objsNum
			cache[cnr] = reportedBefore
			m.Unlock()

			successes++
			l.Debug("successfully put container report to contract",
				zap.Stringer("cid", cnr), zap.Uint64("size", size), zap.Uint64("objectsNum", objsNum))
		}

		l.Debug("sent container reports",
			zap.Int("numOfSuccessReports", successes),
			zap.Int("numOfContainers", len(idList)))
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

func initPaymentChecker(c *cfg) {
	var (
		l = c.log.With(zap.String("component", "paymentChecker"))
		p = &paymentChecker{
			m:          sync.RWMutex{},
			statuses:   make(map[cid.ID]int64),
			balanceCli: c.bCli,
		}
	)

	const changeUnpaidStatusEventName = "ChangePaymentStatus"
	typ := event.TypeFromString(changeUnpaidStatusEventName)
	c.cfgBalance.parsers[typ] = balanceEvent.ParseChangePaymentStatus
	c.cfgBalance.subscribers[typ] = append(c.cfgBalance.subscribers[typ], func(e event.Event) {
		ev := e.(balanceEvent.ChangePaymentStatus)
		p.m.Lock()
		defer p.m.Unlock()

		cID := cid.ID(ev.ContainerID)
		if ev.Unpaid {
			l.Info("container status has changed to unpaid",
				zap.Stringer("cID", cID),
				zap.Uint64("epoch", ev.Epoch))

			p.statuses[cID] = int64(ev.Epoch)
		} else {
			l.Info("container status has changed to paid",
				zap.Stringer("cID", cID),
				zap.Uint64("epoch", ev.Epoch))

			p.statuses[cID] = -1
		}
	})

	c.containerPayments = p
}

type paymentChecker struct {
	m        sync.RWMutex
	statuses map[cid.ID]int64

	balanceCli *balanceClient.Client
}

func (p *paymentChecker) resetCache() {
	p.m.Lock()
	defer p.m.Unlock()

	clear(p.statuses)
}

func (p *paymentChecker) UnpaidSince(cID cid.ID) (int64, error) {
	p.m.RLock()
	epoch, ok := p.statuses[cID]
	p.m.RUnlock()

	if ok {
		return epoch, nil
	}

	p.m.Lock()
	defer p.m.Unlock()
	epoch, ok = p.statuses[cID]
	if ok {
		return epoch, nil
	}

	epoch, err := p.balanceCli.GetUnpaidContainerEpoch(cID)
	if err != nil {
		return 0, fmt.Errorf("FS chain RPC call: %w", err)
	}
	p.statuses[cID] = epoch

	return epoch, nil
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
	if id.IsZero() {
		return x.cnrLst.List(nil)
	}
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
