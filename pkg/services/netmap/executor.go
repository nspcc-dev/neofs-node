package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

type executorSvc struct {
	version *pkg.Version
	state   NodeState
}

// NodeState encapsulates information
// about current node state.
type NodeState interface {
	// Must return current node state
	// in NeoFS API v2 NodeInfo structure.
	LocalNodeInfo() (*netmap.NodeInfo, error)
}

func NewExecutionService(s NodeState, v *pkg.Version) netmap.Service {
	if s == nil || v == nil {
		// this should never happen, otherwise it programmers bug
		panic("can't create netmap execution service")
	}

	return &executorSvc{
		version: v,
		state:   s,
	}
}

func (s *executorSvc) LocalNodeInfo(
	_ context.Context,
	_ *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {
	ni, err := s.state.LocalNodeInfo()
	if err != nil {
		return nil, err
	}

	body := new(netmap.LocalNodeInfoResponseBody)
	body.SetVersion(s.version.ToV2())
	body.SetNodeInfo(ni)

	resp := new(netmap.LocalNodeInfoResponse)
	resp.SetBody(body)

	return resp, nil
}
