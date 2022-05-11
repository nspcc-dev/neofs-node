package netmap

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	versionsdk "github.com/nspcc-dev/neofs-sdk-go/version"
)

type executorSvc struct {
	version refs.Version

	state NodeState

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

func NewExecutionService(s NodeState, v versionsdk.Version, netInfo NetworkInfo) Server {
	if s == nil || netInfo == nil || !version.IsValid(v) {
		// this should never happen, otherwise it programmers bug
		panic("can't create netmap execution service")
	}

	res := &executorSvc{
		state:   s,
		netInfo: netInfo,
	}

	v.WriteToV2(&res.version)

	return res
}

func (s *executorSvc) LocalNodeInfo(
	_ context.Context,
	req *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {
	verV2 := req.GetMetaHeader().GetVersion()
	if verV2 == nil {
		return nil, errors.New("missing version")
	}

	var ver versionsdk.Version
	ver.ReadFromV2(*verV2)

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
	body.SetVersion(&s.version)
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
