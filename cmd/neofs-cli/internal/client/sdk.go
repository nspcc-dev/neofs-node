package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/viper"
)

// GetSDKClientByFlag returns default neofs-sdk-go client using the specified flag for the address.
// On error, outputs to stderr of cmd and exits with non-zero code.
func GetSDKClientByFlag(ctx context.Context, endpointFlag string) (*client.Client, error) {
	return GetSDKClient(ctx, viper.GetString(endpointFlag))
}

// GetSDKClient returns default neofs-sdk-go client.
func GetSDKClient(ctx context.Context, addr string) (*client.Client, error) {
	var (
		prmInit client.PrmInit
	)

	deadline, ok := ctx.Deadline()
	if ok {
		if timeout := time.Until(deadline); timeout > 0 {
			// In CLI we can only set a timeout for the whole operation.
			// By also setting stream timeout we ensure that no operation hands
			// for too long.
			prmInit.SetStreamTimeout(timeout)
		}
	}

	c, err := client.New(prmInit)
	if err != nil {
		return nil, fmt.Errorf("can't create SDK client: %w", err)
	}

	if err := c.DialEndpoint(ctx, addr); err != nil {
		return nil, fmt.Errorf("can't init SDK client: %w", err)
	}

	return c, nil
}

// GetCurrentEpoch returns current epoch.
func GetCurrentEpoch(ctx context.Context, endpoint string) (uint64, error) {
	c, err := GetSDKClient(ctx, endpoint)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	ni, err := c.NetworkInfo(ctx, client.PrmNetworkInfo{})
	if err != nil {
		return 0, err
	}

	return ni.CurrentEpoch(), nil
}
