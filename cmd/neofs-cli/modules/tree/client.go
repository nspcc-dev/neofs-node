package tree

import (
	"context"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// _client returns grpc Tree service client. Should be removed
// after making Tree API public.
func _client(ctx context.Context) (tree.TreeServiceClient, error) {
	var netAddr network.Address
	err := netAddr.FromString(viper.GetString(commonflags.RPC))
	if err != nil {
		return nil, err
	}

	opts := make([]grpc.DialOption, 1, 2)
	opts[0] = grpc.WithBlock()

	if !strings.HasPrefix(netAddr.URIAddr(), "grpcs:") {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// a default connection establishing timeout
	const defaultClientConnectTimeout = time.Second * 2

	ctx, cancel := context.WithTimeout(ctx, defaultClientConnectTimeout)
	cc, err := grpc.DialContext(ctx, netAddr.URIAddr(), opts...)
	cancel()

	return tree.NewTreeServiceClient(cc), err
}
