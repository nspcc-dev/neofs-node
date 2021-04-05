package main

import (
	"bytes"
	"context"
	"encoding/hex"

	v2reputation "github.com/nspcc-dev/neofs-api-go/v2/reputation"
	v2reputationgrpc "github.com/nspcc-dev/neofs-api-go/v2/reputation/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	grpcreputation "github.com/nspcc-dev/neofs-node/pkg/network/transport/reputation/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	reputationrpc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/rpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type localTrustStorage struct {
	log *logger.Logger

	storage *truststorage.Storage

	nmSrc netmapcore.Source

	localKey []byte
}

type localTrustIterator struct {
	ctx trustcontroller.Context

	storage *localTrustStorage

	epochStorage *truststorage.EpochTrustValueStorage
}

func (s *localTrustStorage) InitIterator(ctx trustcontroller.Context) (trustcontroller.Iterator, error) {
	epochStorage, err := s.storage.DataForEpoch(ctx.Epoch())
	if err != nil && !errors.Is(err, truststorage.ErrNoPositiveTrust) {
		return nil, err
	}

	return &localTrustIterator{
		ctx:          ctx,
		storage:      s,
		epochStorage: epochStorage,
	}, nil
}

func (it *localTrustIterator) Iterate(h reputation.TrustHandler) error {
	if it.epochStorage != nil {
		err := it.epochStorage.Iterate(h)
		if !errors.Is(err, truststorage.ErrNoPositiveTrust) {
			return err
		}
	}

	nm, err := it.storage.nmSrc.GetNetMapByEpoch(it.ctx.Epoch())
	if err != nil {
		return err
	}

	// find out if local node is presented in netmap
	localIndex := -1

	for i := range nm.Nodes {
		if bytes.Equal(nm.Nodes[i].PublicKey(), it.storage.localKey) {
			localIndex = i
		}
	}

	ln := len(nm.Nodes)
	if localIndex >= 0 && ln > 0 {
		ln--
	}

	// calculate Pj http://ilpubs.stanford.edu:8090/562/1/2002-56.pdf Chapter 4.5.
	p := reputation.TrustOne.Div(reputation.TrustValueFromInt(ln))

	for i := range nm.Nodes {
		if i == localIndex {
			continue
		}

		trust := reputation.Trust{}
		trust.SetPeer(reputation.PeerIDFromBytes(nm.Nodes[i].PublicKey()))
		trust.SetValue(p)

		if err := h(trust); err != nil {
			return err
		}
	}

	return nil
}

func (s *localTrustStorage) InitWriter(ctx trustcontroller.Context) (trustcontroller.Writer, error) {
	return &localTrustLogger{
		ctx: ctx,
		log: s.log,
	}, nil
}

type localTrustLogger struct {
	ctx trustcontroller.Context

	log *logger.Logger
}

func (l *localTrustLogger) Write(t reputation.Trust) error {
	l.log.Info("new local trust",
		zap.Uint64("epoch", l.ctx.Epoch()),
		zap.String("peer", hex.EncodeToString(t.Peer().Bytes())),
		zap.Stringer("value", t.Value()),
	)

	return nil
}

func (*localTrustLogger) Close() error {
	return nil
}

func initReputationService(c *cfg) {
	// consider sharing this between application components
	nmSrc := newCachedNetmapStorage(c.cfgNetmap.state, c.cfgNetmap.wrapper)

	c.cfgReputation.localTrustStorage = truststorage.New(truststorage.Prm{})

	trustStorage := &localTrustStorage{
		log:      c.log,
		storage:  c.cfgReputation.localTrustStorage,
		nmSrc:    nmSrc,
		localKey: crypto.MarshalPublicKey(&c.key.PublicKey),
	}

	c.cfgReputation.localTrustCtrl = trustcontroller.New(trustcontroller.Prm{
		LocalTrustSource: trustStorage,
		LocalTrustTarget: trustStorage,
	})

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		var reportPrm trustcontroller.ReportPrm

		// report collected values from previous epoch
		reportPrm.SetEpoch(ev.(netmap.NewEpoch).EpochNumber() - 1)

		// TODO: implement and use worker pool [neofs-node#440]
		go c.cfgReputation.localTrustCtrl.Report(reportPrm)
	})

	v2reputationgrpc.RegisterReputationServiceServer(c.cfgGRPC.server,
		grpcreputation.New(
			reputationrpc.NewSignService(
				c.key,
				reputationrpc.NewResponseService(
					&loggingReputationServer{
						log: c.log,
					},
					c.respSvc,
				),
			),
		),
	)
}

type loggingReputationServer struct {
	log *logger.Logger
}

func (s *loggingReputationServer) SendLocalTrust(_ context.Context, req *v2reputation.SendLocalTrustRequest) (*v2reputation.SendLocalTrustResponse, error) {
	body := req.GetBody()

	log := s.log.With(zap.Uint64("epoch", body.GetEpoch()))

	for _, t := range body.GetTrusts() {
		log.Info("local trust received",
			zap.String("peer", hex.EncodeToString(t.GetPeer().GetValue())),
			zap.Float64("value", t.GetValue()),
		)
	}

	resp := new(v2reputation.SendLocalTrustResponse)
	resp.SetBody(new(v2reputation.SendLocalTrustResponseBody))

	return resp, nil
}

func (s *loggingReputationServer) SendIntermediateResult(_ context.Context, req *v2reputation.SendIntermediateResultRequest) (*v2reputation.SendIntermediateResultResponse, error) {
	resp := new(v2reputation.SendIntermediateResultResponse)

	// todo: implement me

	return resp, nil
}
