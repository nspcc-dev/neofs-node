package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type executorSvc struct {
	version       *pkg.Version
	localNodeInfo *netmap.NodeInfo
}

func NewExecutionService(ni *netmap.NodeInfo, v *pkg.Version) netmap.Service {
	if ni == nil || v == nil {
		// this should never happen, otherwise it programmers bug
		panic("can't create netmap execution service")
	}

	return &executorSvc{
		version:       v,
		localNodeInfo: ni,
	}
}

func (s *executorSvc) LocalNodeInfo(
	_ context.Context,
	_ *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {

	body := new(netmap.LocalNodeInfoResponseBody)
	body.SetVersion(s.version.ToV2())
	body.SetNodeInfo(s.localNodeInfo)

	resp := new(netmap.LocalNodeInfoResponse)
	resp.SetBody(body)
	resp.SetMetaHeader(new(session.ResponseMetaHeader))

	return resp, nil
}
