package getsvc

import (
	"context"
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

func (s *Service) forwardGetRequest(ctx context.Context, sortedNodeLists [][]netmap.NodeInfo, forwardRequestFn ForwardGetRequestFunc,
	submitResponseFn SubmitStreamFunc) error {
	for i := range sortedNodeLists {
		for j := range sortedNodeLists[i] {
			conn, node, err := s.conns.(*clientCacheWrapper)._connect(ctx, sortedNodeLists[i][j])
			if err != nil {
				s.log.Debug("get conn to remote node",
					zap.Stringer("addresses", node.AddressGroup()), zap.Error(err))
				continue
			}

			err = forwardRequestFn(ctx, conn)
			if err == nil || errors.Is(err, ctx.Err()) {
				return err
			}

			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Info("failed to GET object from remote node", zap.Error(err))
			}
		}
	}

	return apistatus.ErrObjectNotFound
}

func (s *Service) forwardHeadRequest(ctx context.Context, sortedNodeLists [][]netmap.NodeInfo, forwardRequestFn ForwardHeadRequestFunc,
	submitResponseFn SubmitHeadResponseFunc) error {
	for i := range sortedNodeLists {
		for j := range sortedNodeLists[i] {
			conn, node, err := s.conns.(*clientCacheWrapper)._connect(ctx, sortedNodeLists[i][j])
			if err != nil {
				s.log.Debug("get conn to remote node",
					zap.Stringer("addresses", node.AddressGroup()), zap.Error(err))
				continue
			}

			respBuf, hdr, err := forwardRequestFn(ctx, conn)
			if err == nil {
				submitResponseFn(respBuf, hdr)
				return nil
			}

			if errors.Is(err, ctx.Err()) {
				return err
			}

			if !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Info("failed to HEAD object from remote node", zap.Error(err))
			}
		}
	}

	return apistatus.ErrObjectNotFound
}
