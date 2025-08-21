package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// List returns a list of container identifiers belonging
// to the specified user of NeoFS system. The list is composed
// through Container contract call.
//
// Returns the identifiers of all NeoFS containers if pointer
// to user identifier is nil.
func (c *Client) List(idUser *user.ID) ([]cid.ID, error) {
	var rawIdUser []byte
	if idUser != nil {
		rawIdUser = idUser[:]
	}

	res, err := c.client.TestInvokeIterator(listMethod, iteratorPrefetchNumber, rawIdUser)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listMethod, err)
	}

	cidList := make([]cid.ID, 0, len(res))
	for i := range res {
		rawID, err := client.BytesFromStackItem(res[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", listMethod, err)
		}

		var id cid.ID

		err = id.Decode(rawID)
		if err != nil {
			return nil, fmt.Errorf("decode container ID: %w", err)
		}

		cidList = append(cidList, id)
	}

	return cidList, nil
}
