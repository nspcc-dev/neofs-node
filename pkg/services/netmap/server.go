package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
)

// Server is an interface of the NeoFS API Netmap service server
type Server interface {
	LocalNodeInfo(context.Context, *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error)
	NetworkInfo(context.Context, *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error)
}
