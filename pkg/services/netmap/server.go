package netmap

import (
	"context"
	"crypto/ecdsa"

	apinetmap "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	protonetmap "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	apirefs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

// Contract groups ops of the Netmap contract deployed in the FS chain required
// to serve NeoFS API Netmap service.
type Contract interface {
	netmapcore.State
	// LocalNodeInfo returns local node's settings along with its network status.
	LocalNodeInfo() (netmap.NodeInfo, error)
	// GetNetworkInfo returns the current network configuration.
	GetNetworkInfo() (netmap.NetworkInfo, error)
	// GetNetworkMap current network map.
	GetNetworkMap() (netmap.NetMap, error)
}

type server struct {
	protonetmap.UnimplementedNetmapServiceServer
	signer   *ecdsa.PrivateKey
	contract Contract
}

// New provides protocontainer.NetmapServiceServer based on specified
// [Contract].
//
// All response messages are signed using specified signer and have current
// epoch in the meta header.
func New(s *ecdsa.PrivateKey, c Contract) protonetmap.NetmapServiceServer {
	return &server{
		signer:   s,
		contract: c,
	}
}

func currentProtoVersion() *refs.Version {
	v := version.Current()
	var v2 apirefs.Version
	v.WriteToV2(&v2)
	return v2.ToGRPCMessage().(*refs.Version)
}

func (s *server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: currentProtoVersion(),
		Epoch:   s.contract.CurrentEpoch(),
		Status:  st,
	}
}

func (s *server) makeNodeInfoResponse(body *protonetmap.LocalNodeInfoResponse_Body, st *protostatus.Status) (*protonetmap.LocalNodeInfoResponse, error) {
	resp := &protonetmap.LocalNodeInfoResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	return util.SignResponse(s.signer, resp, apinetmap.LocalNodeInfoResponse{}), nil
}

func (s *server) makeStatusNodeInfoResponse(err error) (*protonetmap.LocalNodeInfoResponse, error) {
	return s.makeNodeInfoResponse(nil, util.ToStatus(err))
}

// LocalNodeInfo returns current state of the local node from the underlying
// [NodeState].
func (s server) LocalNodeInfo(_ context.Context, req *protonetmap.LocalNodeInfoRequest) (*protonetmap.LocalNodeInfoResponse, error) {
	nodeInfoReq := new(apinetmap.LocalNodeInfoRequest)
	if err := nodeInfoReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	if err := signature.VerifyServiceMessage(nodeInfoReq); err != nil {
		return s.makeStatusNodeInfoResponse(util.ToRequestSignatureVerificationError(err))
	}

	n, err := s.contract.LocalNodeInfo()
	if err != nil {
		return s.makeStatusNodeInfoResponse(err)
	}

	var n2 apinetmap.NodeInfo
	n.WriteToV2(&n2)
	body := &protonetmap.LocalNodeInfoResponse_Body{
		Version:  currentProtoVersion(),
		NodeInfo: n2.ToGRPCMessage().(*protonetmap.NodeInfo),
	}
	return s.makeNodeInfoResponse(body, util.StatusOK)
}

func (s *server) makeNetInfoResponse(body *protonetmap.NetworkInfoResponse_Body, st *protostatus.Status) (*protonetmap.NetworkInfoResponse, error) {
	resp := &protonetmap.NetworkInfoResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	return util.SignResponse(s.signer, resp, apinetmap.NetworkInfoResponse{}), nil
}

func (s *server) makeStatusNetInfoResponse(err error) (*protonetmap.NetworkInfoResponse, error) {
	return s.makeNetInfoResponse(nil, util.ToStatus(err))
}

// NetworkInfo returns current network configuration from the underlying
// [Contract].
func (s *server) NetworkInfo(_ context.Context, req *protonetmap.NetworkInfoRequest) (*protonetmap.NetworkInfoResponse, error) {
	netInfoReq := new(apinetmap.NetworkInfoRequest)
	if err := netInfoReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	if err := signature.VerifyServiceMessage(netInfoReq); err != nil {
		return s.makeStatusNetInfoResponse(util.ToRequestSignatureVerificationError(err))
	}

	n, err := s.contract.GetNetworkInfo()
	if err != nil {
		return s.makeStatusNetInfoResponse(err)
	}

	var n2 apinetmap.NetworkInfo
	n.WriteToV2(&n2)
	body := &protonetmap.NetworkInfoResponse_Body{
		NetworkInfo: n2.ToGRPCMessage().(*protonetmap.NetworkInfo),
	}
	return s.makeNetInfoResponse(body, util.StatusOK)
}

func (s *server) makeNetmapResponse(body *protonetmap.NetmapSnapshotResponse_Body, st *protostatus.Status) (*protonetmap.NetmapSnapshotResponse, error) {
	resp := &protonetmap.NetmapSnapshotResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	return util.SignResponse(s.signer, resp, apinetmap.SnapshotResponse{}), nil
}

func (s *server) makeStatusNetmapResponse(err error) (*protonetmap.NetmapSnapshotResponse, error) {
	return s.makeNetmapResponse(nil, util.ToStatus(err))
}

// NetmapSnapshot returns current network map from the underlying [Contract].
func (s *server) NetmapSnapshot(_ context.Context, req *protonetmap.NetmapSnapshotRequest) (*protonetmap.NetmapSnapshotResponse, error) {
	snapshotReq := new(apinetmap.SnapshotRequest)
	if err := snapshotReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}
	if err := signature.VerifyServiceMessage(snapshotReq); err != nil {
		return s.makeStatusNetmapResponse(util.ToRequestSignatureVerificationError(err))
	}

	n, err := s.contract.GetNetworkMap()
	if err != nil {
		return s.makeStatusNetmapResponse(err)
	}

	var n2 apinetmap.NetMap
	n.WriteToV2(&n2)
	body := &protonetmap.NetmapSnapshotResponse_Body{
		Netmap: n2.ToGRPCMessage().(*protonetmap.Netmap),
	}
	return s.makeNetmapResponse(body, util.StatusOK)
}
