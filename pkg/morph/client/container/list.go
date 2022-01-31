package container

import (
	"fmt"

	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

// List returns a list of container identifiers belonging
// to the specified owner of NeoFS system. The list is composed
// through Container contract call.
//
// Returns the identifiers of all NeoFS containers if pointer
// to owner identifier is nil.
func (c *Client) List(ownerID *owner.ID) ([]*cid.ID, error) {
	var rawID []byte
	if ownerID == nil {
		rawID = []byte{}
	} else if v2 := ownerID.ToV2(); v2 == nil {
		return nil, errUnsupported // use other major version if there any
	} else {
		rawID = v2.GetValue()
	}

	prm := client.TestInvokePrm{}
	prm.SetMethod(listMethod)
	prm.SetArgs(rawID)

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listMethod, err)
	} else if ln := len(res); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", listMethod, ln)
	}

	res, err = client.ArrayFromStackItem(res[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", listMethod, err)
	}

	cidList := make([]*cid.ID, 0, len(res))
	for i := range res {
		rawCid, err := client.BytesFromStackItem(res[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", listMethod, err)
		}

		v2 := new(v2refs.ContainerID)
		v2.SetValue(rawCid)

		cidList = append(cidList, cid.NewFromV2(v2))
	}

	return cidList, nil
}
