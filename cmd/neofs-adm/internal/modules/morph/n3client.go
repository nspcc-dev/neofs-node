package morph

import (
	"context"
	"errors"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type clientContext struct {
	Client       *client.Client
	Hashes       []util.Uint256
	WaitDuration time.Duration
	PollInterval time.Duration
}

func getN3Client(v *viper.Viper) (*client.Client, error) {
	// number of opened connections
	// by neo-go client per one host
	const maxConnsPerHost = 10

	ctx := context.Background() // FIXME(@fyrchik): timeout context
	endpoint := v.GetString(endpointFlag)
	if endpoint == "" {
		return nil, errors.New("missing endpoint")
	}
	c, err := client.New(ctx, endpoint, client.Options{MaxConnsPerHost: maxConnsPerHost})
	if err != nil {
		return nil, err
	}
	if err := c.Init(); err != nil {
		return nil, err
	}
	return c, nil
}

func defaultClientContext(c *client.Client) *clientContext {
	return &clientContext{
		Client:       c,
		WaitDuration: time.Second * 30,
		PollInterval: time.Second,
	}
}

func (c *clientContext) sendTx(tx *transaction.Transaction, cmd *cobra.Command, await bool) error {
	h, err := c.Client.SendRawTransaction(tx)
	if err != nil {
		return err
	}

	c.Hashes = append(c.Hashes, h)

	if await {
		return c.awaitTx(cmd)
	}
	return nil
}
