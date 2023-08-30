package netmap

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// GetNetMap calls "snapshot" method and decodes netmap.NetMap from the response.
func (c *Client) GetNetMap(diff uint64) (*netmap.NetMap, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(snapshotMethod)
	prm.SetArgs(diff)

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, err
	}

	return DecodeNetMap(res)
}
