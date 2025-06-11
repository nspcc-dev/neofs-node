package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"errors"
	"fmt"

	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	containerCore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	netmapEv "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	containerService "github.com/nspcc-dev/neofs-node/pkg/services/container"
	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	loadroute "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/route"
	placementrouter "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/route/placement"
	loadstorage "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	apiClient "github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protocontainer "github.com/nspcc-dev/neofs-sdk-go/proto/container"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.uber.org/zap"
)

const (
	startEstimationNotifyEvent = "StartEstimation"
	stopEstimationNotifyEvent  = "StopEstimation"
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

	estimationsLogger := c.log.With(zap.String("component", "container_estimations"))

	localMetrics := &localStorageLoad{
		log:    estimationsLogger,
		engine: c.cfgObject.cfgLocalStorage.localStorage,
	}

	pubKey := c.key.PublicKey().Bytes()

	resultWriter := &morphLoadWriter{
		log:            estimationsLogger,
		cnrMorphClient: c.cCli,
		key:            pubKey,
	}

	loadAccumulator := loadstorage.New(containerrpc.CleanupDelta)

	addNewEpochAsyncNotificationHandler(c, func(e event.Event) {
		ev := e.(netmapEv.NewEpoch)
		loadAccumulator.EpochEvent(ev.EpochNumber())
	})

	loadPlacementBuilder := &loadPlacementBuilder{
		log:    estimationsLogger,
		nmSrc:  c.netMapSource,
		cnrSrc: c.cnrSrc,
	}

	routeBuilder := placementrouter.New(placementrouter.Prm{
		PlacementBuilder: loadPlacementBuilder,
	})

	loadRouter := loadroute.New(
		loadroute.Prm{
			LocalServerInfo: c,
			RemoteWriterProvider: &remoteLoadAnnounceProvider{
				key:             &c.key.PrivateKey,
				netmapKeys:      c,
				clientCache:     c.bgClientCache,
				deadEndProvider: loadcontroller.SimpleWriterProvider(loadAccumulator),
			},
			Builder: routeBuilder,
		},
		loadroute.WithLogger(estimationsLogger),
	)

	ctrl := loadcontroller.New(
		loadcontroller.Prm{
			LocalMetrics:            loadcontroller.SimpleIteratorProvider(localMetrics),
			AnnouncementAccumulator: loadcontroller.SimpleIteratorProvider(loadAccumulator),
			LocalAnnouncementTarget: loadRouter,
			ResultReceiver:          loadcontroller.SimpleWriterProvider(resultWriter),
		},
		loadcontroller.WithLogger(estimationsLogger),
	)

	setContainerNotificationParser(c, startEstimationNotifyEvent, containerEvent.ParseStartEstimation)
	addContainerAsyncNotificationHandler(c, startEstimationNotifyEvent, func(ev event.Event) {
		ctrl.Start(loadcontroller.StartPrm{
			Epoch: ev.(containerEvent.StartEstimation).Epoch(),
		})
	})

	setContainerNotificationParser(c, stopEstimationNotifyEvent, containerEvent.ParseStopEstimation)
	addContainerAsyncNotificationHandler(c, stopEstimationNotifyEvent, func(ev event.Event) {
		ctrl.Stop(loadcontroller.StopPrm{
			Epoch: ev.(containerEvent.StopEstimation).Epoch(),
		})
	})

	server := &usedSpaceService{
		ContainerServiceServer: containerService.New(&c.key.PrivateKey, c.networkState, c.cli, (*containersInChain)(&c.basics), c.nCli),
		loadWriterProvider:     loadRouter,
		loadPlacementBuilder:   loadPlacementBuilder,
		routeBuilder:           routeBuilder,
		cfg:                    c,
	}

	for _, srv := range c.cfgGRPC.servers {
		protocontainer.RegisterContainerServiceServer(srv, server)
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

type morphLoadWriter struct {
	log *zap.Logger

	cnrMorphClient *cntClient.Client

	key []byte
}

func (w *morphLoadWriter) Put(a containerSDK.SizeEstimation) error {
	w.log.Debug("save used space announcement in contract",
		zap.Uint64("epoch", a.Epoch()),
		zap.Stringer("cid", a.Container()),
		zap.Uint64("size", a.Value()),
	)

	prm := cntClient.AnnounceLoadPrm{}

	prm.SetAnnouncement(a)
	prm.SetReporter(w.key)

	return w.cnrMorphClient.AnnounceLoad(prm)
}

func (*morphLoadWriter) Close() error {
	return nil
}

type nopLoadWriter struct{}

func (nopLoadWriter) Put(containerSDK.SizeEstimation) error {
	return nil
}

func (nopLoadWriter) Close() error {
	return nil
}

type remoteLoadAnnounceProvider struct {
	key *ecdsa.PrivateKey

	netmapKeys netmapCore.AnnouncedKeys

	clientCache interface {
		Get(client.NodeInfo) (client.MultiAddressClient, error)
	}

	deadEndProvider loadcontroller.WriterProvider
}

func (r *remoteLoadAnnounceProvider) InitRemote(srv loadroute.ServerInfo) (loadcontroller.WriterProvider, error) {
	if srv == nil {
		return r.deadEndProvider, nil
	}

	if r.netmapKeys.IsLocalKey(srv.PublicKey()) {
		// if local => return no-op writer
		return loadcontroller.SimpleWriterProvider(new(nopLoadWriter)), nil
	}

	var info client.NodeInfo

	err := client.NodeInfoFromRawNetmapElement(&info, srv)
	if err != nil {
		return nil, fmt.Errorf("parse client node info: %w", err)
	}

	c, err := r.clientCache.Get(info)
	if err != nil {
		return nil, fmt.Errorf("could not initialize API client: %w", err)
	}

	return &remoteLoadAnnounceWriterProvider{
		client: c,
	}, nil
}

type remoteLoadAnnounceWriterProvider struct {
	client client.Client
}

func (p *remoteLoadAnnounceWriterProvider) InitWriter(ctx context.Context) (loadcontroller.Writer, error) {
	return &remoteLoadAnnounceWriter{
		ctx:    ctx,
		client: p.client,
	}, nil
}

type remoteLoadAnnounceWriter struct {
	ctx context.Context

	client client.Client

	buf []containerSDK.SizeEstimation
}

func (r *remoteLoadAnnounceWriter) Put(a containerSDK.SizeEstimation) error {
	r.buf = append(r.buf, a)

	return nil
}

func (r *remoteLoadAnnounceWriter) Close() error {
	var cliPrm apiClient.PrmAnnounceSpace
	return r.client.ContainerAnnounceUsedSpace(r.ctx, r.buf, cliPrm)
}

type loadPlacementBuilder struct {
	log *zap.Logger

	nmSrc netmapCore.Source

	cnrSrc containerCore.Source
}

func (l *loadPlacementBuilder) BuildPlacement(epoch uint64, cnr cid.ID) ([][]netmap.NodeInfo, error) {
	cnrNodes, nm, err := l.buildPlacement(epoch, cnr)
	if err != nil {
		return nil, err
	}

	buf := cnr[:]

	binary.LittleEndian.PutUint64(buf, epoch)

	var pivot oid.ID
	_ = pivot.Decode(buf)

	placement, err := nm.PlacementVectors(cnrNodes, pivot)
	if err != nil {
		return nil, fmt.Errorf("could not build placement vectors: %w", err)
	}

	return placement, nil
}

func (l *loadPlacementBuilder) buildPlacement(epoch uint64, idCnr cid.ID) ([][]netmap.NodeInfo, *netmap.NetMap, error) {
	cnr, err := l.cnrSrc.Get(idCnr)
	if err != nil {
		return nil, nil, err
	}

	nm, err := l.nmSrc.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get network map: %w", err)
	}

	cnrNodes, err := nm.ContainerNodes(cnr.PlacementPolicy(), idCnr)
	if err != nil {
		return nil, nil, fmt.Errorf("could not build container nodes: %w", err)
	}

	return cnrNodes, nm, nil
}

type localStorageLoad struct {
	log *zap.Logger

	engine *engine.StorageEngine
}

func (d *localStorageLoad) Iterate(f loadcontroller.UsedSpaceFilter, h loadcontroller.UsedSpaceHandler) error {
	idList, err := d.engine.ListContainers()
	if err != nil {
		return fmt.Errorf("list containers on engine failure: %w", err)
	}

	for i := range idList {
		sz, err := d.engine.ContainerSize(idList[i])
		if err != nil {
			d.log.Debug("failed to calculate container size in storage engine",
				zap.Stringer("cid", idList[i]),
				zap.Error(err),
			)

			continue
		}

		d.log.Debug("container size in storage engine calculated successfully",
			zap.Uint64("size", sz),
			zap.Stringer("cid", idList[i]),
		)

		var a containerSDK.SizeEstimation
		a.SetContainer(idList[i])
		a.SetValue(sz)

		if f != nil && !f(a) {
			continue
		}

		if err := h(a); err != nil {
			return err
		}
	}

	return nil
}

type usedSpaceService struct {
	protocontainer.ContainerServiceServer

	loadWriterProvider loadcontroller.WriterProvider

	loadPlacementBuilder *loadPlacementBuilder

	routeBuilder loadroute.Builder

	cfg *cfg
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

func (c *usedSpaceService) PublicKey() []byte {
	return nodeKeyFromNetmap(c.cfg)
}

func (c *usedSpaceService) IterateAddresses(f func(string) bool) {
	c.cfg.iterateNetworkAddresses(f)
}

func (c *usedSpaceService) NumberOfAddresses() int {
	return c.cfg.addressNum()
}

func (c *usedSpaceService) makeResponse(body *protocontainer.AnnounceUsedSpaceResponse_Body, st *protostatus.Status, req *protocontainer.AnnounceUsedSpaceRequest) (*protocontainer.AnnounceUsedSpaceResponse, error) {
	resp := &protocontainer.AnnounceUsedSpaceResponse{
		Body: body,
		MetaHeader: &protosession.ResponseMetaHeader{
			Version: version.Current().ProtoMessage(),
			Epoch:   c.cfg.networkState.CurrentEpoch(),
			Status:  st,
		},
	}
	resp.VerifyHeader = util.SignResponse(&c.cfg.key.PrivateKey, resp, req)
	return resp, nil
}

func (c *usedSpaceService) makeStatusResponse(err error, req *protocontainer.AnnounceUsedSpaceRequest) (*protocontainer.AnnounceUsedSpaceResponse, error) {
	return c.makeResponse(nil, util.ToStatus(err), req)
}

func (c *usedSpaceService) AnnounceUsedSpace(ctx context.Context, req *protocontainer.AnnounceUsedSpaceRequest) (*protocontainer.AnnounceUsedSpaceResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return c.makeStatusResponse(err, req)
	}

	var passedRoute []loadroute.ServerInfo

	for hdr := req.GetVerifyHeader(); hdr != nil; hdr = hdr.GetOrigin() {
		passedRoute = append(passedRoute, &containerOnlyKeyRemoteServerInfo{
			key: hdr.GetBodySignature().GetKey(),
		})
	}

	for left, right := 0, len(passedRoute)-1; left < right; left, right = left+1, right-1 {
		passedRoute[left], passedRoute[right] = passedRoute[right], passedRoute[left]
	}

	passedRoute = append(passedRoute, c)

	w, err := c.loadWriterProvider.InitWriter(loadroute.NewRouteContext(ctx, passedRoute))
	if err != nil {
		return c.makeStatusResponse(fmt.Errorf("could not initialize container's used space writer: %w", err), req)
	}

	var est containerSDK.SizeEstimation

	for _, a := range req.GetBody().GetAnnouncements() {
		err = est.FromProtoMessage(a)
		if err != nil {
			return c.makeStatusResponse(fmt.Errorf("invalid size announcement: %w", err), req)
		}

		if err := c.processLoadValue(ctx, est, passedRoute, w); err != nil {
			return c.makeStatusResponse(err, req)
		}
	}

	return c.makeResponse(nil, util.StatusOK, req)
}

var errNodeOutsideContainer = errors.New("node outside the container")

type containerOnlyKeyRemoteServerInfo struct {
	key []byte
}

func (i *containerOnlyKeyRemoteServerInfo) PublicKey() []byte {
	return i.key
}

func (*containerOnlyKeyRemoteServerInfo) IterateAddresses(func(string) bool) {
}

func (*containerOnlyKeyRemoteServerInfo) NumberOfAddresses() int {
	return 0
}

func (l *loadPlacementBuilder) isNodeFromContainerKey(epoch uint64, cnr cid.ID, key []byte) (bool, error) {
	cnrNodes, _, err := l.buildPlacement(epoch, cnr)
	if err != nil {
		return false, err
	}

	for i := range cnrNodes {
		for j := range cnrNodes[i] {
			if bytes.Equal(cnrNodes[i][j].PublicKey(), key) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (c *usedSpaceService) processLoadValue(_ context.Context, a containerSDK.SizeEstimation,
	route []loadroute.ServerInfo, w loadcontroller.Writer) error {
	fromCnr, err := c.loadPlacementBuilder.isNodeFromContainerKey(a.Epoch(), a.Container(), route[0].PublicKey())
	if err != nil {
		return fmt.Errorf("could not verify that the sender belongs to the container: %w", err)
	} else if !fromCnr {
		return errNodeOutsideContainer
	}

	err = loadroute.CheckRoute(c.routeBuilder, a, route)
	if err != nil {
		return fmt.Errorf("wrong route of container's used space value: %w", err)
	}

	err = w.Put(a)
	if err != nil {
		return fmt.Errorf("could not write container's used space value: %w", err)
	}

	return nil
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
