package audit

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Audit contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Audit contract client
}

const (
	putResultMethod          = "put"
	getResultMethod          = "get"
	listResultsMethod        = "list"
	listByEpochResultsMethod = "listByEpoch"
	listByCIDResultsMethod   = "listByCID"
	listByNodeResultsMethod  = "listByNode"
)

// NewFromMorph returns the wrapper instance from the raw morph client.
func NewFromMorph(cli *client.Client, contract util.Uint160, opts ...client.StaticClientOption) (*Client, error) {
	sc, err := client.NewStatic(cli, contract, opts...)
	if err != nil {
		return nil, fmt.Errorf("could not create static client of audit contract: %w", err)
	}

	return &Client{client: sc}, nil
}
