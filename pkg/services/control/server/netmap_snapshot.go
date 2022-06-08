package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	netmapAPI "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NetmapSnapshot reads network map snapshot from Netmap storage.
func (s *Server) NetmapSnapshot(ctx context.Context, req *control.NetmapSnapshotRequest) (*control.NetmapSnapshotResponse, error) {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// get current epoch
	epoch, err := s.netMapSrc.Epoch()
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	apiNetMap, err := s.netMapSrc.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	nm := new(control.Netmap)
	nm.SetEpoch(epoch)
	nm.SetNodes(nodesFromAPI(apiNetMap.Nodes()))

	// create and fill response
	resp := new(control.NetmapSnapshotResponse)

	body := new(control.NetmapSnapshotResponse_Body)
	resp.SetBody(body)

	body.SetNetmap(nm)

	// sign the response
	if err := SignMessage(s.key, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

func nodesFromAPI(apiNodes []netmapAPI.NodeInfo) []*control.NodeInfo {
	nodes := make([]*control.NodeInfo, 0, len(apiNodes))

	for i := range apiNodes {
		node := new(control.NodeInfo)
		node.SetPublicKey(apiNodes[i].PublicKey())

		addrs := make([]string, 0, apiNodes[i].NumberOfNetworkEndpoints())
		netmapAPI.IterateNetworkEndpoints(apiNodes[i], func(s string) {
			addrs = append(addrs, s)
		})
		node.SetAddresses(addrs)
		node.SetAttributes(attributesFromAPI(apiNodes[i]))

		switch {
		default:
			node.SetState(control.NetmapStatus_STATUS_UNDEFINED)
		case apiNodes[i].IsOnline():
			node.SetState(control.NetmapStatus_ONLINE)
		case apiNodes[i].IsOffline():
			node.SetState(control.NetmapStatus_OFFLINE)
		}

		nodes = append(nodes, node)
	}

	return nodes
}

func attributesFromAPI(apiNode netmapAPI.NodeInfo) []*control.NodeInfo_Attribute {
	attrs := make([]*control.NodeInfo_Attribute, 0, apiNode.NumberOfAttributes())

	apiNode.IterateAttributes(func(key, value string) {
		a := new(control.NodeInfo_Attribute)
		a.SetKey(key)
		a.SetValue(value)

		attrs = append(attrs, a)
	})

	return attrs
}
