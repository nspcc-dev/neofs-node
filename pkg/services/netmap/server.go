package netmap

import (
	"context"
	"crypto/ecdsa"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	protonetmap "github.com/nspcc-dev/neofs-sdk-go/proto/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
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
	return version.Current().ProtoMessage()
}

func (s *server) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: currentProtoVersion(),
		Epoch:   s.contract.CurrentEpoch(),
		Status:  st,
	}
}

func (s *server) makeNodeInfoResponse(body *protonetmap.LocalNodeInfoResponse_Body, st *protostatus.Status, req *protonetmap.LocalNodeInfoRequest) (*protonetmap.LocalNodeInfoResponse, error) {
	resp := &protonetmap.LocalNodeInfoResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeStatusNodeInfoResponse(err error, req *protonetmap.LocalNodeInfoRequest) (*protonetmap.LocalNodeInfoResponse, error) {
	return s.makeNodeInfoResponse(nil, util.ToStatus(err), req)
}

// LocalNodeInfo returns current state of the local node from the underlying
// [NodeState].
func (s server) LocalNodeInfo(_ context.Context, req *protonetmap.LocalNodeInfoRequest) (*protonetmap.LocalNodeInfoResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeStatusNodeInfoResponse(err, req)
	}

	n, err := s.contract.LocalNodeInfo()
	if err != nil {
		return s.makeStatusNodeInfoResponse(err, req)
	}

	body := &protonetmap.LocalNodeInfoResponse_Body{
		Version:  currentProtoVersion(),
		NodeInfo: n.ProtoMessage(),
	}
	return s.makeNodeInfoResponse(body, util.StatusOK, req)
}

func (s *server) makeNetInfoResponse(body *protonetmap.NetworkInfoResponse_Body, st *protostatus.Status, req *protonetmap.NetworkInfoRequest) (*protonetmap.NetworkInfoResponse, error) {
	resp := &protonetmap.NetworkInfoResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeStatusNetInfoResponse(err error, req *protonetmap.NetworkInfoRequest) (*protonetmap.NetworkInfoResponse, error) {
	return s.makeNetInfoResponse(nil, util.ToStatus(err), req)
}

// NetworkInfo returns current network configuration from the underlying
// [Contract].
func (s *server) NetworkInfo(_ context.Context, req *protonetmap.NetworkInfoRequest) (*protonetmap.NetworkInfoResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeStatusNetInfoResponse(err, req)
	}

	n, err := s.contract.GetNetworkInfo()
	if err != nil {
		return s.makeStatusNetInfoResponse(err, req)
	}

	body := &protonetmap.NetworkInfoResponse_Body{
		NetworkInfo: n.ProtoMessage(),
	}
	return s.makeNetInfoResponse(body, util.StatusOK, req)
}

func (s *server) makeNetmapResponse(body *protonetmap.NetmapSnapshotResponse_Body, st *protostatus.Status, req *protonetmap.NetmapSnapshotRequest) (*protonetmap.NetmapSnapshotResponse, error) {
	resp := &protonetmap.NetmapSnapshotResponse{
		Body:       body,
		MetaHeader: s.makeResponseMetaHeader(st),
	}
	resp.VerifyHeader = util.SignResponse(s.signer, resp, req)
	return resp, nil
}

func (s *server) makeStatusNetmapResponse(err error, req *protonetmap.NetmapSnapshotRequest) (*protonetmap.NetmapSnapshotResponse, error) {
	return s.makeNetmapResponse(nil, util.ToStatus(err), req)
}

// NetmapSnapshot returns current network map from the underlying [Contract].
func (s *server) NetmapSnapshot(_ context.Context, req *protonetmap.NetmapSnapshotRequest) (*protonetmap.NetmapSnapshotResponse, error) {
	if err := icrypto.VerifyRequestSignatures(req); err != nil {
		return s.makeStatusNetmapResponse(err, req)
	}

	n, err := s.contract.GetNetworkMap()
	if err != nil {
		return s.makeStatusNetmapResponse(err, req)
	}

	body := &protonetmap.NetmapSnapshotResponse_Body{
		Netmap: n.ProtoMessage(),
	}
	return s.makeNetmapResponse(body, util.StatusOK, req)
}
