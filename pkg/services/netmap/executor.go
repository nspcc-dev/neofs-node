package netmap

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
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

	// ReadCurrentNetMap reads current local network map of the storage node
	// into the given parameter. Returns any error encountered which prevented
	// the network map to be read.
	ReadCurrentNetMap(*netmap.NetMap) error
}

// NetworkInfo encapsulates source of the
// recent information about the NeoFS network.
type NetworkInfo interface {
	// Must return recent network information in NeoFS API v2 NetworkInfo structure.
	//
	// If protocol version is <=2.9, MillisecondsPerBlock and network config should be unset.
	Dump(versionsdk.Version) (*netmapSDK.NetworkInfo, error)
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
	if err := ver.ReadFromV2(*verV2); err != nil {
		return nil, fmt.Errorf("can't read version: %w", err)
	}

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
	verV2 := req.GetMetaHeader().GetVersion()
	if verV2 == nil {
		return nil, errors.New("missing protocol version in meta header")
	}

	var ver versionsdk.Version
	if err := ver.ReadFromV2(*verV2); err != nil {
		return nil, fmt.Errorf("can't read version: %w", err)
	}

	ni, err := s.netInfo.Dump(ver)
	if err != nil {
		return nil, err
	}

	var niV2 netmap.NetworkInfo
	ni.WriteToV2(&niV2)

	body := new(netmap.NetworkInfoResponseBody)
	body.SetNetworkInfo(&niV2)

	resp := new(netmap.NetworkInfoResponse)
	resp.SetBody(body)

	return resp, nil
}

func (s *executorSvc) Snapshot(_ context.Context, _ *netmap.SnapshotRequest) (*netmap.SnapshotResponse, error) {
	var nm netmap.NetMap

	err := s.state.ReadCurrentNetMap(&nm)
	if err != nil {
		return nil, fmt.Errorf("read current local network map: %w", err)
	}

	body := new(netmap.SnapshotResponseBody)
	body.SetNetMap(&nm)

	resp := new(netmap.SnapshotResponse)
	resp.SetBody(body)

	return resp, nil
}
