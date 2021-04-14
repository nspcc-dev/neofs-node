package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"strconv"

	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	containerV2 "github.com/nspcc-dev/neofs-api-go/v2/container"
	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	containerCore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	containerTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/container/grpc"
	containerService "github.com/nspcc-dev/neofs-node/pkg/services/container"
	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	loadroute "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/route"
	placementrouter "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/route/placement"
	loadstorage "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/storage"
	containerMorph "github.com/nspcc-dev/neofs-node/pkg/services/container/morph"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	startEstimationNotifyEvent = "StartEstimation"
	stopEstimationNotifyEvent  = "StopEstimation"
)

func initContainerService(c *cfg) {
	staticClient, err := client.NewStatic(
		c.cfgMorph.client,
		c.cfgContainer.scriptHash,
		c.cfgContainer.fee,
	)
	fatalOnErr(err)

	cnrClient, err := container.New(staticClient)
	fatalOnErr(err)

	wrap, err := wrapper.New(cnrClient)
	fatalOnErr(err)

	c.cfgObject.cnrStorage = newCachedContainerStorage(wrap) // use RPC node as source of containers (with caching)
	c.cfgObject.cnrClient = wrap

	localMetrics := &localStorageLoad{
		log:    c.log,
		engine: c.cfgObject.cfgLocalStorage.localStorage,
	}

	pubKey := crypto.MarshalPublicKey(&c.key.PublicKey)

	resultWriter := &morphLoadWriter{
		log:            c.log,
		cnrMorphClient: wrap,
		key:            pubKey,
	}

	loadAccumulator := loadstorage.New(loadstorage.Prm{})

	loadPlacementBuilder := &loadPlacementBuilder{
		log:    c.log,
		nmSrc:  c.cfgNetmap.wrapper,
		cnrSrc: wrap,
	}

	routeBuilder := placementrouter.New(placementrouter.Prm{
		PlacementBuilder: loadPlacementBuilder,
	})

	loadRouter := loadroute.New(
		loadroute.Prm{
			LocalServerInfo: c,
			RemoteWriterProvider: &remoteLoadAnnounceProvider{
				key:             c.key,
				loadAddrSrc:     c,
				clientCache:     cache.NewSDKClientCache(), // FIXME: use shared cache
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
	addContainerNotificationHandler(c, startEstimationNotifyEvent, func(ev event.Event) {
		ctrl.Start(loadcontroller.StartPrm{
			Epoch: ev.(containerEvent.StartEstimation).Epoch(),
		})
	})

	setContainerNotificationParser(c, stopEstimationNotifyEvent, containerEvent.ParseStopEstimation)
	addContainerNotificationHandler(c, stopEstimationNotifyEvent, func(ev event.Event) {
		ctrl.Stop(loadcontroller.StopPrm{
			Epoch: ev.(containerEvent.StopEstimation).Epoch(),
		})
	})

	containerGRPC.RegisterContainerServiceServer(c.cfgGRPC.server,
		containerTransportGRPC.New(
			containerService.NewSignService(
				c.key,
				containerService.NewResponseService(
					&usedSpaceService{
						Server:               containerService.NewExecutionService(containerMorph.NewExecutor(cnrClient)),
						loadWriterProvider:   loadRouter,
						loadPlacementBuilder: loadPlacementBuilder,
						routeBuilder:         routeBuilder,
						cfg:                  c,
					},
					c.respSvc,
				),
			),
		),
	)
}

// addContainerNotificationHandler adds handler that will be executed synchronously
func addContainerNotificationHandler(c *cfg, sTyp string, h event.Handler) {
	typ := event.TypeFromString(sTyp)

	if c.cfgContainer.subscribers == nil {
		c.cfgContainer.subscribers = make(map[event.Type][]event.Handler, 1)
	}

	c.cfgContainer.subscribers[typ] = append(c.cfgContainer.subscribers[typ], h)
}

// addContainerAsyncNotificationHandler adds handler that will be executed asynchronously via container workerPool
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

func setContainerNotificationParser(c *cfg, sTyp string, p event.Parser) {
	typ := event.TypeFromString(sTyp)

	if c.cfgContainer.parsers == nil {
		c.cfgContainer.parsers = make(map[event.Type]event.Parser, 1)
	}

	c.cfgContainer.parsers[typ] = p
}

type morphLoadWriter struct {
	log *logger.Logger

	cnrMorphClient *wrapper.Wrapper

	key []byte
}

func (w *morphLoadWriter) Put(a containerSDK.UsedSpaceAnnouncement) error {
	w.log.Debug("save used space announcement in contract",
		zap.Uint64("epoch", a.Epoch()),
		zap.Stringer("cid", a.ContainerID()),
		zap.Uint64("size", a.UsedSpace()),
	)

	return w.cnrMorphClient.AnnounceLoad(a, w.key)
}

func (*morphLoadWriter) Close() error {
	return nil
}

type nopLoadWriter struct{}

func (nopLoadWriter) Put(containerSDK.UsedSpaceAnnouncement) error {
	return nil
}

func (nopLoadWriter) Close() error {
	return nil
}

type remoteLoadAnnounceProvider struct {
	key *ecdsa.PrivateKey

	loadAddrSrc network.LocalAddressSource

	clientCache interface {
		Get(string) (apiClient.Client, error)
	}

	deadEndProvider loadcontroller.WriterProvider
}

func (r *remoteLoadAnnounceProvider) InitRemote(srv loadroute.ServerInfo) (loadcontroller.WriterProvider, error) {
	if srv == nil {
		return r.deadEndProvider, nil
	}

	addr := srv.Address()

	if r.loadAddrSrc.LocalAddress().String() == srv.Address() {
		// if local => return no-op writer
		return loadcontroller.SimpleWriterProvider(new(nopLoadWriter)), nil
	}

	ipAddr, err := network.IPAddrFromMultiaddr(addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert address to IP format")
	}

	c, err := r.clientCache.Get(ipAddr)
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize API client")
	}

	return &remoteLoadAnnounceWriterProvider{
		client: c,
		key:    r.key,
	}, nil
}

type remoteLoadAnnounceWriterProvider struct {
	client apiClient.Client
	key    *ecdsa.PrivateKey
}

func (p *remoteLoadAnnounceWriterProvider) InitWriter(ctx context.Context) (loadcontroller.Writer, error) {
	return &remoteLoadAnnounceWriter{
		ctx:    ctx,
		client: p.client,
		key:    p.key,
	}, nil
}

type remoteLoadAnnounceWriter struct {
	ctx context.Context

	client apiClient.Client
	key    *ecdsa.PrivateKey

	buf []containerSDK.UsedSpaceAnnouncement
}

func (r *remoteLoadAnnounceWriter) Put(a containerSDK.UsedSpaceAnnouncement) error {
	r.buf = append(r.buf, a)

	return nil
}

func (r *remoteLoadAnnounceWriter) Close() error {
	return r.client.AnnounceContainerUsedSpace(r.ctx, r.buf, apiClient.WithKey(r.key))
}

type loadPlacementBuilder struct {
	log *logger.Logger

	nmSrc netmapCore.Source

	cnrSrc containerCore.Source
}

func (l *loadPlacementBuilder) BuildPlacement(epoch uint64, cid *containerSDK.ID) ([]netmap.Nodes, error) {
	cnrNodes, nm, err := l.buildPlacement(epoch, cid)
	if err != nil {
		return nil, err
	}

	const pivotPrefix = "load_announcement_"

	pivot := []byte(
		pivotPrefix + strconv.FormatUint(epoch, 10),
	)

	placement, err := nm.GetPlacementVectors(cnrNodes, pivot)
	if err != nil {
		return nil, errors.Wrap(err, "could not build placement vectors")
	}

	return placement, nil
}

func (l *loadPlacementBuilder) buildPlacement(epoch uint64, cid *containerSDK.ID) (netmap.ContainerNodes, *netmap.Netmap, error) {
	cnr, err := l.cnrSrc.Get(cid)
	if err != nil {
		return nil, nil, err
	}

	nm, err := l.nmSrc.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not get network map")
	}

	cnrNodes, err := nm.GetContainerNodes(cnr.PlacementPolicy(), cid.ToV2().GetValue())
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not build container nodes")
	}

	return cnrNodes, nm, nil
}

type localStorageLoad struct {
	log *logger.Logger

	engine *engine.StorageEngine
}

func (d *localStorageLoad) Iterate(f loadcontroller.UsedSpaceFilter, h loadcontroller.UsedSpaceHandler) error {
	idList := engine.ListContainers(d.engine)

	for i := range idList {
		sz := engine.ContainerSize(d.engine, idList[i])

		d.log.Debug("container size in storage engine calculated successfully",
			zap.Uint64("size", sz),
			zap.Stringer("cid", idList[i]),
		)

		a := containerSDK.NewAnnouncement()
		a.SetContainerID(idList[i])
		a.SetUsedSpace(sz)

		if f != nil && !f(*a) {
			continue
		}

		if err := h(*a); err != nil {
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
	ni := c.localNodeInfo()
	return ni.PublicKey()
}

func (c *cfg) Address() string {
	ni := c.localNodeInfo()
	return ni.Address()
}

func (c *usedSpaceService) PublicKey() []byte {
	ni := c.cfg.localNodeInfo()

	return ni.PublicKey()
}

func (c *usedSpaceService) Address() string {
	ni := c.cfg.localNodeInfo()

	return ni.Address()
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
		return nil, errors.Wrap(err, "could not initialize container's used space writer")
	}

	for _, aV2 := range req.GetBody().GetAnnouncements() {
		if err := c.processLoadValue(ctx, *containerSDK.NewAnnouncementFromV2(aV2), passedRoute, w); err != nil {
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

func (*containerOnlyKeyRemoteServerInfo) Address() string {
	return ""
}

func (l *loadPlacementBuilder) isNodeFromContainerKey(epoch uint64, cid *containerSDK.ID, key []byte) (bool, error) {
	cnrNodes, _, err := l.buildPlacement(epoch, cid)
	if err != nil {
		return false, err
	}

	for _, vector := range cnrNodes.Replicas() {
		for _, node := range vector {
			if bytes.Equal(node.PublicKey(), key) {
				return true, nil
			}
		}
	}

	return false, nil
}

func (c *usedSpaceService) processLoadValue(ctx context.Context, a containerSDK.UsedSpaceAnnouncement,
	route []loadroute.ServerInfo, w loadcontroller.Writer) error {
	fromCnr, err := c.loadPlacementBuilder.isNodeFromContainerKey(a.Epoch(), a.ContainerID(), route[0].PublicKey())
	if err != nil {
		return errors.Wrap(err, "could not verify that the sender belongs to the container")
	} else if !fromCnr {
		return errNodeOutsideContainer
	}

	err = loadroute.CheckRoute(c.routeBuilder, a, route)
	if err != nil {
		return errors.Wrap(err, "wrong route of container's used space value")
	}

	err = w.Put(a)
	if err != nil {
		return errors.Wrap(err, "could not write container's used space value")
	}

	return nil
}
