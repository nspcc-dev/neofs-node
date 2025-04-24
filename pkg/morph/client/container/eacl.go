package container

import (
	"fmt"
	"strings"

	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
)

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
//
// Returns apistatus.EACLNotFound if eACL table is missing in the contract.
func (c *Client) GetEACL(cnr cid.ID) (*container.EACL, error) {
	prm := client.TestInvokePrm{}
	method := eaclDataMethod
	prm.SetMethod(method)
	prm.SetArgs(cnr[:])

	arr, err := c.client.TestInvoke(prm)
	old := err != nil && isMethodNotFoundError(err, method)
	if old {
		method = eaclMethod
		prm.SetMethod(method)
		arr, err = c.client.TestInvoke(prm)
	}
	if err != nil {
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			return nil, apistatus.ErrContainerNotFound
		}
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", method, err)
	} else if ln := len(arr); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}

	if old {
		arr, err = client.ArrayFromStackItem(arr[0])
		if err != nil {
			return nil, fmt.Errorf("could not get item array of eACL (%s): %w", eaclMethod, err)
		}

		if len(arr) == 0 {
			return nil, fmt.Errorf("unexpected eacl stack item count (%s): %d", eaclMethod, len(arr))
		}
	}

	rawEACL, err := client.BytesFromStackItem(arr[0])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of eACL (%s): %w", method, err)
	}

	if len(rawEACL) == 0 {
		var errEACLNotFound apistatus.EACLNotFound

		return nil, errEACLNotFound
	}

	var res container.EACL

	t, err := eacl.Unmarshal(rawEACL)
	if err != nil {
		return nil, err
	}
	res.Value = &t

	return &res, nil
}
