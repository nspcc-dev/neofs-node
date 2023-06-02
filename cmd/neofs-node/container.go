package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/hrw"
	containerV2 "github.com/nspcc-dev/neofs-api-go/v2/container"
	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	containerCore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	containerTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/container/grpc"
	containerService "github.com/nspcc-dev/neofs-node/pkg/services/container"
	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	loadroute "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/route"
	placementrouter "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/route/placement"
	loadstorage "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/storage"
	containerMorph "github.com/nspcc-dev/neofs-node/pkg/services/container/morph"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	apiClient "github.com/nspcc-dev/neofs-sdk-go/client"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

const (
	startEstimationNotifyEvent = "StartEstimation"
	stopEstimationNotifyEvent  = "StopEstimation"
)

func initContainerService(c *cfg) {
	// container wrapper that tries to invoke notary
	// requests if chain is configured so
	wrap, err := cntClient.NewFromMorph(c.cfgMorph.client, c.cfgContainer.scriptHash, 0)
	fatalOnErr(err)

	c.shared.cnrClient = wrap

	// container wrapper that always sends non-notary
	// requests
	wrapperNoNotary, err := cntClient.NewFromMorph(c.cfgMorph.client, c.cfgContainer.scriptHash, 0)
	fatalOnErr(err)

	cnrSrc := cntClient.AsContainerSource(wrap)

	eACLFetcher := &morphEACLFetcher{
		w: wrap,
	}

	cnrRdr := new(morphContainerReader)

	cnrWrt := &morphContainerWriter{
		neoClient: wrap,
	}

	if c.cfgMorph.cacheTTL <= 0 {
		c.cfgObject.eaclSource = eACLFetcher
		cnrRdr.eacl = eACLFetcher
		c.cfgObject.cnrSource = cnrSrc
		cnrRdr.get = cnrSrc
		cnrRdr.lister = wrap
	} else {
		// use RPC node as source of Container contract items (with caching)
		cachedContainerStorage := newCachedContainerStorage(cnrSrc, c.cfgMorph.cacheTTL)
		cachedEACLStorage := newCachedEACLStorage(eACLFetcher, c.cfgMorph.cacheTTL)
		cachedContainerLister := newCachedContainerLister(wrap, c.cfgMorph.cacheTTL)

		subscribeToContainerCreation(c, func(e event.Event) {
			ev := e.(containerEvent.PutSuccess)

			// read owner of the created container in order to update the reading cache.
			// TODO: use owner directly from the event after neofs-contract#256 will become resolved
			//  but don't forget about the profit of reading the new container and caching it:
			//  creation success are most commonly tracked by polling GET op.
			cnr, err := cachedContainerStorage.Get(ev.ID)
			if err == nil {
				cachedContainerLister.update(cnr.Value.Owner(), ev.ID, true)
			} else {
				// unlike removal, we expect successful receive of the container
				// after successful creation, so logging can be useful
				c.log.Error("read newly created container after the notification",
					zap.Stringer("id", ev.ID),
					zap.Error(err),
				)
			}

			c.log.Debug("container creation event's receipt",
				zap.Stringer("id", ev.ID),
			)
		})

		subscribeToContainerRemoval(c, func(e event.Event) {
			ev := e.(containerEvent.DeleteSuccess)

			// read owner of the removed container in order to update the listing cache.
			// It's strange to read already removed container, but we can successfully hit
			// the cache.
			// TODO: use owner directly from the event after neofs-contract#256 will become resolved
			cnr, err := cachedContainerStorage.Get(ev.ID)
			if err == nil {
				cachedContainerLister.update(cnr.Value.Owner(), ev.ID, false)
			}

			cachedContainerStorage.handleRemoval(ev.ID)

			c.log.Debug("container removal event's receipt",
				zap.Stringer("id", ev.ID),
			)
		})

		c.cfgObject.eaclSource = cachedEACLStorage
		c.cfgObject.cnrSource = cachedContainerStorage

		cnrRdr.lister = cachedContainerLister
		cnrRdr.eacl = c.cfgObject.eaclSource
		cnrRdr.get = c.cfgObject.cnrSource

		cnrWrt.cacheEnabled = true
		cnrWrt.eacls = cachedEACLStorage
	}

	localMetrics := &localStorageLoad{
		log:    c.log,
		engine: c.cfgObject.cfgLocalStorage.localStorage,
	}

	pubKey := c.key.PublicKey().Bytes()

	resultWriter := &morphLoadWriter{
		log:            c.log,
		cnrMorphClient: wrapperNoNotary,
		key:            pubKey,
	}

	loadAccumulator := loadstorage.New(loadstorage.Prm{})

	loadPlacementBuilder := &loadPlacementBuilder{
		log:    c.log,
		nmSrc:  c.netMapSource,
		cnrSrc: cnrSrc,
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
		loadroute.WithLogger(c.log),
	)

	ctrl := loadcontroller.New(
		loadcontroller.Prm{
			LocalMetrics:            loadcontroller.SimpleIteratorProvider(localMetrics),
			AnnouncementAccumulator: loadcontroller.SimpleIteratorProvider(loadAccumulator),
			LocalAnnouncementTarget: loadRouter,
			ResultReceiver:          loadcontroller.SimpleWriterProvider(resultWriter),
		},
		loadcontroller.WithLogger(c.log),
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

	server := containerTransportGRPC.New(
		containerService.NewSignService(
			&c.key.PrivateKey,
			containerService.NewResponseService(
				&usedSpaceService{
					Server:               containerService.NewExecutionService(containerMorph.NewExecutor(cnrRdr, cnrWrt)),
					loadWriterProvider:   loadRouter,
					loadPlacementBuilder: loadPlacementBuilder,
					routeBuilder:         routeBuilder,
					cfg:                  c,
				},
				c.respSvc,
			),
		),
	)

	for _, srv := range c.cfgGRPC.servers {
		containerGRPC.RegisterContainerServiceServer(srv, server)
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
// similar functions.
func subscribeToContainerCreation(c *cfg, h event.Handler) {
	const eventNameContainerCreated = "PutSuccess"
	registerEventParserOnceContainer(c, eventNameContainerCreated, containerEvent.ParsePutSuccess)
	addContainerAsyncNotificationHandler(c, eventNameContainerCreated, h)
}

// like subscribeToContainerCreation but for removal.
func subscribeToContainerRemoval(c *cfg, h event.Handler) {
	const eventNameContainerRemoved = "DeleteSuccess"
	registerEventParserOnceContainer(c, eventNameContainerRemoved, containerEvent.ParseDeleteSuccess)
	addContainerAsyncNotificationHandler(c, eventNameContainerRemoved, h)
}

func setContainerNotificationParser(c *cfg, sTyp string, p event.NotificationParser) {
	typ := event.TypeFromString(sTyp)

	if c.cfgContainer.parsers == nil {
		c.cfgContainer.parsers = make(map[event.Type]event.NotificationParser, 1)
	}

	c.cfgContainer.parsers[typ] = p
}

type morphLoadWriter struct {
	log *logger.Logger

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
		Get(client.NodeInfo) (client.Client, error)
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

	cliPrm.SetValues(r.buf)

	_, err := r.client.ContainerAnnounceUsedSpace(r.ctx, cliPrm)
	return err
}

type loadPlacementBuilder struct {
	log *logger.Logger

	nmSrc netmapCore.Source

	cnrSrc containerCore.Source
}

func (l *loadPlacementBuilder) BuildPlacement(epoch uint64, cnr cid.ID) ([][]netmap.NodeInfo, error) {
	cnrNodes, nm, err := l.buildPlacement(epoch, cnr)
	if err != nil {
		return nil, err
	}

	const pivotPrefix = "load_announcement_"

	pivot := []byte(
		pivotPrefix + strconv.FormatUint(epoch, 10),
	)

	return placementVectors(cnrNodes, nm, hrw.Hash(pivot)), nil
}

func placementVectors(cnrNodes [][]netmap.NodeInfo, nm *netmap.NetMap, hash uint64) [][]netmap.NodeInfo {
	minimumPrice := minPrice(nm.Nodes())
	meanCap := meanCapacity(nm.Nodes())
	sortedVectors := make([][]netmap.NodeInfo, len(cnrNodes))

	for i, v := range cnrNodes {
		sortedVectors[i] = make([]netmap.NodeInfo, len(v))
		copy(sortedVectors[i], v)

		hrw.SortSliceByWeightValue(sortedVectors[i], weights(sortedVectors[i], minimumPrice, meanCap), hash)
	}

	return sortedVectors
}

func weights(nn []netmap.NodeInfo, minPrice, meanCap float64) []float64 {
	w := make([]float64, 0, len(nn))
	for _, n := range nn {
		w = append(w, reverseMinNorm(float64(n.Price()), minPrice)*sigmoidNorm(float64(capacity(n)), meanCap))
	}

	return w
}

func reverseMinNorm(v, base float64) float64 {
	if v == 0 || base == 0 {
		return 0
	}

	return base / v
}

func sigmoidNorm(v, base float64) float64 {
	if v == 0 || base == 0 {
		return 0
	}

	x := v / base

	return x / (1 + x)
}

func meanCapacity(nn []netmap.NodeInfo) float64 {
	l := len(nn)
	if l == 0 {
		return 0
	}

	var sum uint64
	for _, n := range nn {
		sum += capacity(n)
	}

	return float64(sum) / float64(l)
}

func minPrice(nn []netmap.NodeInfo) float64 {
	l := len(nn)
	if l == 0 {
		return 0
	}

	var min uint64
	for i := 0; i < l; i++ {
		if p := nn[i].Price(); min == 0 || p < min { // That is how min looks in SDK in RC8: https://github.com/nspcc-dev/neofs-sdk-go/issues/438
			min = p
		}
	}

	return float64(min)
}

// capacity is copied with minor changed from SDK.
// See https://github.com/nspcc-dev/neofs-sdk-go/blob/v1.0.0-rc.8/netmap/node_info.go#L283.
func capacity(n netmap.NodeInfo) uint64 {
	val := n.Attribute("Capacity")
	if val == "" {
		return 0
	}

	capParsed, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0
	}

	return capParsed
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

	cnrNodes, err := nm.ContainerNodes(cnr.Value.PlacementPolicy(), idCnr)
	if err != nil {
		return nil, nil, fmt.Errorf("could not build container nodes: %w", err)
	}

	return cnrNodes, nm, nil
}

type localStorageLoad struct {
	log *logger.Logger

	engine *engine.StorageEngine
}

func (d *localStorageLoad) Iterate(f loadcontroller.UsedSpaceFilter, h loadcontroller.UsedSpaceHandler) error {
	idList, err := engine.ListContainers(d.engine)
	if err != nil {
		return fmt.Errorf("list containers on engine failure: %w", err)
	}

	for i := range idList {
		sz, err := engine.ContainerSize(d.engine, idList[i])
		if err != nil {
			d.log.Debug("failed to calculate container size in storage engine",
				zap.Stringer("cid", idList[i]),
				zap.String("error", err.Error()),
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
	containerService.Server

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

func (c *cfg) ExternalAddresses() []string {
	return c.cfgNodeInfo.localInfo.ExternalAddresses()
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

func (c *usedSpaceService) ExternalAddresses() []string {
	return c.cfg.ExternalAddresses()
}

func (c *usedSpaceService) AnnounceUsedSpace(ctx context.Context, req *containerV2.AnnounceUsedSpaceRequest) (*containerV2.AnnounceUsedSpaceResponse, error) {
	var passedRoute []loadroute.ServerInfo

	for hdr := req.GetVerificationHeader(); hdr != nil; hdr = hdr.GetOrigin() {
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
		return nil, fmt.Errorf("could not initialize container's used space writer: %w", err)
	}

	var est containerSDK.SizeEstimation

	for _, aV2 := range req.GetBody().GetAnnouncements() {
		err = est.ReadFromV2(aV2)
		if err != nil {
			return nil, fmt.Errorf("invalid size announcement: %w", err)
		}

		if err := c.processLoadValue(ctx, est, passedRoute, w); err != nil {
			return nil, err
		}
	}

	respBody := new(containerV2.AnnounceUsedSpaceResponseBody)

	resp := new(containerV2.AnnounceUsedSpaceResponse)
	resp.SetBody(respBody)

	return resp, nil
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

func (*containerOnlyKeyRemoteServerInfo) ExternalAddresses() []string {
	return nil
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

// implements interface required by container service provided by morph executor.
type morphContainerReader struct {
	eacl containerCore.EACLSource

	get containerCore.Source

	lister interface {
		List(*user.ID) ([]cid.ID, error)
	}
}

func (x *morphContainerReader) Get(id cid.ID) (*containerCore.Container, error) {
	return x.get.Get(id)
}

func (x *morphContainerReader) GetEACL(id cid.ID) (*containerCore.EACL, error) {
	return x.eacl.GetEACL(id)
}

func (x *morphContainerReader) List(id *user.ID) ([]cid.ID, error) {
	return x.lister.List(id)
}

type morphContainerWriter struct {
	neoClient *cntClient.Client

	cacheEnabled bool
	eacls        *ttlEACLStorage
}

func (m morphContainerWriter) Put(cnr containerCore.Container) (*cid.ID, error) {
	return cntClient.Put(m.neoClient, cnr)
}

func (m morphContainerWriter) Delete(witness containerCore.RemovalWitness) error {
	return cntClient.Delete(m.neoClient, witness)
}

func (m morphContainerWriter) PutEACL(eaclInfo containerCore.EACL) error {
	err := cntClient.PutEACL(m.neoClient, eaclInfo)
	if err != nil {
		return err
	}

	if m.cacheEnabled {
		id, _ := eaclInfo.Value.CID()
		m.eacls.InvalidateEACL(id)
	}

	return nil
}
