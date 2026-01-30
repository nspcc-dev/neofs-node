package n3util

import (
	"context"
	"errors"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/spf13/viper"
)

// GetN3Client creates and initializes neo-go RPC client using configuration from viper.
// It reads the endpoint from the provided viper instance using the key "rpc-endpoint".
func GetN3Client(v *viper.Viper) (*rpcclient.Client, error) {
	// number of opened connections
	// by neo-go client per one host
	const (
		maxConnsPerHost = 10
		requestTimeout  = time.Second * 10
	)

	ctx := context.Background()
	endpoint := v.GetString("rpc-endpoint")
	if endpoint == "" {
		return nil, errors.New("missing endpoint")
	}
	c, err := rpcclient.New(ctx, endpoint, rpcclient.Options{
		MaxConnsPerHost: maxConnsPerHost,
		RequestTimeout:  requestTimeout,
	})
	if err != nil {
		return nil, err
	}
	if err := c.Init(); err != nil {
		return nil, err
	}
	return c, nil
}
