package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var errInvalidEndpoint = errors.New("provided RPC endpoint is incorrect")

// GetSDKClientByFlag returns default neofs-sdk-go client using the specified flag for the address.
// On error, outputs to stderr of cmd and exits with non-zero code.
func GetSDKClientByFlag(ctx context.Context, cmd *cobra.Command, endpointFlag string) *client.Client {
	cli, err := getSDKClientByFlag(ctx, cmd, endpointFlag)
	if err != nil {
		common.ExitOnErr(cmd, "can't create API client: %w", err)
	}
	return cli
}

func getSDKClientByFlag(ctx context.Context, cmd *cobra.Command, endpointFlag string) (*client.Client, error) {
	var addr network.Address

	err := addr.FromString(viper.GetString(endpointFlag))
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errInvalidEndpoint, err)
	}
	return GetSDKClient(ctx, cmd, addr)
}

// GetSDKClient returns default neofs-sdk-go client.
func GetSDKClient(ctx context.Context, cmd *cobra.Command, addr network.Address) (*client.Client, error) {
	var (
		prmInit client.PrmInit
		prmDial client.PrmDial
	)

	prmDial.SetServerURI(addr.URIAddr())
	prmDial.SetContext(ctx)

	deadline, ok := ctx.Deadline()
	if ok {
		if timeout := time.Until(deadline); timeout > 0 {
			// In CLI we can only set a timeout for the whole operation.
			// By also setting stream timeout we ensure that no operation hands
			// for too long.
			prmDial.SetTimeout(timeout)
			prmDial.SetStreamTimeout(timeout)
		}
	}

	c, err := client.New(prmInit)
	if err != nil {
		return nil, fmt.Errorf("can't create SDK client: %w", err)
	}

	if err := c.Dial(prmDial); err != nil { //nolint:contextcheck // SetContext is used above.
		return nil, fmt.Errorf("can't init SDK client: %w", err)
	}

	return c, nil
}

// GetCurrentEpoch returns current epoch.
func GetCurrentEpoch(ctx context.Context, cmd *cobra.Command, endpoint string) (uint64, error) {
	var addr network.Address

	if err := addr.FromString(endpoint); err != nil {
		return 0, fmt.Errorf("can't parse RPC endpoint: %w", err)
	}

	c, err := GetSDKClient(ctx, cmd, addr)
	if err != nil {
		return 0, err
	}

	ni, err := c.NetworkInfo(ctx, client.PrmNetworkInfo{})
	if err != nil {
		return 0, err
	}

	return ni.CurrentEpoch(), nil
}
