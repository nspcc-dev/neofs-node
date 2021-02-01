package main

import (
	"context"

	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	containerTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/container/grpc"
	containerService "github.com/nspcc-dev/neofs-node/pkg/services/container"
	loadcontroller "github.com/nspcc-dev/neofs-node/pkg/services/container/announcement/load/controller"
	containerMorph "github.com/nspcc-dev/neofs-node/pkg/services/container/morph"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
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

	c.cfgObject.cnrStorage = wrap // use RPC node as source of containers
	c.cfgObject.cnrClient = wrap

	containerGRPC.RegisterContainerServiceServer(c.cfgGRPC.server,
		containerTransportGRPC.New(
			containerService.NewSignService(
				c.key,
				containerService.NewResponseService(
					containerService.NewExecutionService(
						containerMorph.NewExecutor(cnrClient),
					),
					c.respSvc,
				),
			),
		),
	)
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

type remoteLoadAnnounceWriterProvider struct {
	client *apiClient.Client
}

func (p *remoteLoadAnnounceWriterProvider) InitWriter(ctx context.Context) (loadcontroller.Writer, error) {
	return &remoteLoadAnnounceWriter{
		ctx:    ctx,
		client: p.client,
	}, nil
}

type remoteLoadAnnounceWriter struct {
	ctx context.Context

	client *apiClient.Client

	buf []containerSDK.UsedSpaceAnnouncement
}

func (r *remoteLoadAnnounceWriter) Put(a containerSDK.UsedSpaceAnnouncement) error {
	r.buf = append(r.buf, a)

	return nil
}

func (r *remoteLoadAnnounceWriter) Close() error {
	return r.client.AnnounceContainerUsedSpace(r.ctx, r.buf)
}
