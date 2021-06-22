package control

import (
	"context"

	netmapAPI "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
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
	nm.SetNodes(nodesFromAPI(apiNetMap.Nodes))

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

func nodesFromAPI(apiNodes netmapAPI.Nodes) []*control.NodeInfo {
	nodes := make([]*control.NodeInfo, 0, len(apiNodes))

	for _, apiNode := range apiNodes {
		node := new(control.NodeInfo)
		node.SetPublicKey(apiNode.PublicKey())

		addrs := make([]string, apiNode.NumberOfAddresses())
		netmapAPI.IterateAllAddresses(apiNode.NodeInfo, func(s string) {
			addrs = append(addrs, s)
		})
		node.SetAddresses(addrs)
		node.SetAttributes(attributesFromAPI(apiNode.Attributes()))
		node.SetState(stateFromAPI(apiNode.State()))

		nodes = append(nodes, node)
	}

	return nodes
}

func stateFromAPI(s netmapAPI.NodeState) control.NetmapStatus {
	switch s {
	default:
		return control.NetmapStatus_STATUS_UNDEFINED
	case netmapAPI.NodeStateOffline:
		return control.NetmapStatus_OFFLINE
	case netmapAPI.NodeStateOnline:
		return control.NetmapStatus_ONLINE
	}
}

func attributesFromAPI(apiAttrs []*netmapAPI.NodeAttribute) []*control.NodeInfo_Attribute {
	attrs := make([]*control.NodeInfo_Attribute, 0, len(apiAttrs))

	for _, apiAttr := range apiAttrs {
		a := new(control.NodeInfo_Attribute)
		a.SetKey(apiAttr.Key())
		a.SetValue(apiAttr.Value())

		apiParents := apiAttr.ParentKeys()
		parents := make([]string, 0, len(apiParents))

		for i := range apiParents {
			parents = append(parents, apiParents[i])
		}

		a.SetParents(parents)

		attrs = append(attrs, a)
	}

	return attrs
}
