package netmap

import "github.com/nspcc-dev/neo-go/pkg/crypto/keys"

// SetInnerRing updates inner ring members in netmap contract.
func (c *Client) SetInnerRing(keys keys.PublicKeys) error {
	args := make([][]byte, len(keys))
	for i := range args {
		args[i] = keys[i].Bytes()
	}

	return c.client.Invoke(c.setInnerRing, args)
}
