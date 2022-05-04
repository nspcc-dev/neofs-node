package internal

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
)

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
