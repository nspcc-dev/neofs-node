package internal

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var errInvalidEndpoint = errors.New("provided RPC endpoint is incorrect")

// GetSDKClientByFlag returns default neofs-sdk-go client using the specified flag for the address.
// On error, outputs to stderr of cmd and exits with non-zero code.
func GetSDKClientByFlag(cmd *cobra.Command, key *ecdsa.PrivateKey, endpointFlag string) *client.Client {
	cli, err := getSDKClientByFlag(key, endpointFlag)
	if err != nil {
		common.ExitOnErr(cmd, "can't create API client: %w", err)
	}
	return cli
}

func getSDKClientByFlag(key *ecdsa.PrivateKey, endpointFlag string) (*client.Client, error) {
	var addr network.Address

	err := addr.FromString(viper.GetString(endpointFlag))
	if err != nil {
		return nil, fmt.Errorf("%v: %w", errInvalidEndpoint, err)
	}
	return GetSDKClient(key, addr)
}

// GetSDKClient returns default neofs-sdk-go client.
func GetSDKClient(key *ecdsa.PrivateKey, addr network.Address) (*client.Client, error) {
	var (
		c       client.Client
		prmInit client.PrmInit
		prmDial client.PrmDial
	)

	prmInit.SetDefaultPrivateKey(*key)
	prmInit.ResolveNeoFSFailures()
	prmDial.SetServerURI(addr.URIAddr())

	c.Init(prmInit)

	if err := c.Dial(prmDial); err != nil {
		return nil, fmt.Errorf("can't init SDK client: %w", err)
	}

	return &c, nil
}
