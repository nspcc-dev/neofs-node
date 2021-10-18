package morph

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/spf13/viper"
)

func getN3Client(v *viper.Viper) (*client.Client, error) {
	ctx := context.Background() // FIXME(@fyrchik): timeout context
	endpoint := v.GetString(endpointFlag)
	if endpoint == "" {
		return nil, errors.New("missing endpoint")
	}
	c, err := client.New(ctx, endpoint, client.Options{})
	if err != nil {
		return nil, err
	}
	if err := c.Init(); err != nil {
		return nil, err
	}
	return c, nil
}
