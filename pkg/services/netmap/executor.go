package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

type executorSvc struct {
	version *version.Version
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
	// Must return recent network information in NeoFS API v2 NetworkInfo structure.
	//
	// If protocol version is <=2.9, MillisecondsPerBlock and network config should be unset.
	Dump(*refs.Version) (*netmap.NetworkInfo, error)
}

func NewExecutionService(s NodeState, v *version.Version, netInfo NetworkInfo) Server {
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
	req *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {
	ver := version.NewFromV2(req.GetMetaHeader().GetVersion())

	ni, err := s.state.LocalNodeInfo()
	if err != nil {
		return nil, err
	}

	if addrNum := ni.NumberOfAddresses(); addrNum > 0 && ver.Minor() <= 7 {
		ni2 := new(netmap.NodeInfo)
		ni2.SetPublicKey(ni.GetPublicKey())
		ni2.SetState(ni.GetState())
		ni2.SetAttributes(ni.GetAttributes())
		ni.IterateAddresses(func(s string) bool {
			ni2.SetAddresses(s)
			return true
		})

		ni = ni2
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
	req *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error) {
	ni, err := s.netInfo.Dump(req.GetMetaHeader().GetVersion())
	if err != nil {
		return nil, err
	}

	body := new(netmap.NetworkInfoResponseBody)
	body.SetNetworkInfo(ni)

	resp := new(netmap.NetworkInfoResponse)
	resp.SetBody(body)

	return resp, nil
}
