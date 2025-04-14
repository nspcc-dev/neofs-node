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
	"github.com/nspcc-dev/neofs-sdk-go/session"
)

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
//
// Returns apistatus.EACLNotFound if eACL table is missing in the contract.
func (c *Client) GetEACL(cnr cid.ID) (*container.EACL, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(eaclMethod)
	prm.SetArgs(cnr[:])

	prms, err := c.client.TestInvoke(prm)
	if err != nil {
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			return nil, apistatus.ErrContainerNotFound
		}
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", eaclMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", eaclMethod, ln)
	}

	arr, err := client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get item array of eACL (%s): %w", eaclMethod, err)
	}

	if len(arr) != 4 {
		return nil, fmt.Errorf("unexpected eacl stack item count (%s): %d", eaclMethod, len(arr))
	}

	rawEACL, err := client.BytesFromStackItem(arr[0])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of eACL (%s): %w", eaclMethod, err)
	}

	sig, err := client.BytesFromStackItem(arr[1])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of eACL signature (%s): %w", eaclMethod, err)
	}

	if len(rawEACL) == 0 {
		var errEACLNotFound apistatus.EACLNotFound

		return nil, errEACLNotFound
	}

	pub, err := client.BytesFromStackItem(arr[2])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of eACL public key (%s): %w", eaclMethod, err)
	}

	binToken, err := client.BytesFromStackItem(arr[3])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of eACL session token (%s): %w", eaclMethod, err)
	}

	var res container.EACL

	t, err := eacl.Unmarshal(rawEACL)
	if err != nil {
		return nil, err
	}
	res.Value = &t

	if len(binToken) > 0 {
		res.Session = new(session.Container)

		err = res.Session.Unmarshal(binToken)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal session token: %w", err)
		}
	}

	res.Signature, err = decodeSignature(pub, sig)
	if err != nil {
		return nil, fmt.Errorf("decode signature: %w", err)
	}

	return &res, nil
}
