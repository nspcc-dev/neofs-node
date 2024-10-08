package tree

import (
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// _client returns grpc Tree service client. Should be removed
// after making Tree API public.
func _client() (tree.TreeServiceClient, error) {
	var netAddr network.Address
	err := netAddr.FromString(viper.GetString(commonflags.RPC))
	if err != nil {
		return nil, err
	}

	opts := make([]grpc.DialOption, 0, 1)

	if !strings.HasPrefix(netAddr.URIAddr(), "grpcs:") {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	cc, err := grpc.NewClient(netAddr.URIAddr(), opts...)

	return tree.NewTreeServiceClient(cc), err
}
