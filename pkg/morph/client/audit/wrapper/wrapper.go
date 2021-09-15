package audit

import (
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/internal"
)

// ClientWrapper is a wrapper over Audit contract
// client which implements storage of audit results.
type ClientWrapper struct {
	internal.StaticClient

	client *audit.Client
}

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8, opts ...client.StaticClientOption) (*ClientWrapper, error) {
	staticClient, err := client.NewStatic(cli, contract, fee, opts...)
	if err != nil {
		return nil, err
	}

	return &ClientWrapper{
		StaticClient: staticClient,
		client:       audit.New(staticClient),
	}, nil
}
