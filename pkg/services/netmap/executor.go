package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

type executorSvc struct {
	version *pkg.Version
	state   NodeState

	netInfo NetworkInfo
}

// NodeState encapsulates information
// about current node state.
type NodeState interface {
	// Must return current node state
	// in NeoFS API v2 NodeInfo structure.
	LocalNodeInfo() (*netmap.NodeInfo, error)
}

// NetworkInfo encapsulates source of the
// recent information about the NeoFS network.
type NetworkInfo interface {
	// Must return recent network information.
	// in NeoFS API v2 NetworkInfo structure.
	Dump() (*netmap.NetworkInfo, error)
}

func NewExecutionService(s NodeState, v *pkg.Version, netInfo NetworkInfo) Server {
	if s == nil || v == nil || netInfo == nil {
		// this should never happen, otherwise it programmers bug
		panic("can't create netmap execution service")
	}

	return &executorSvc{
		version: v,
		state:   s,
		netInfo: netInfo,
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

func (s *executorSvc) NetworkInfo(
	_ context.Context,
	_ *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error) {
	ni, err := s.netInfo.Dump()
	if err != nil {
		return nil, err
	}

	body := new(netmap.NetworkInfoResponseBody)
	body.SetNetworkInfo(ni)

	resp := new(netmap.NetworkInfoResponse)
	resp.SetBody(body)

	return resp, nil
}
