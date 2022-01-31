package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/signature"
)

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
func (c *Client) GetEACL(cid *cid.ID) (*eacl.Table, error) {
	if cid == nil {
		return nil, errNilArgument
	}

	v2 := cid.ToV2()
	if v2 == nil {
		return nil, errUnsupported // use other major version if there any
	}

	prm := client.TestInvokePrm{}
	prm.SetMethod(eaclMethod)
	prm.SetArgs(v2.GetValue())

	prms, err := c.client.TestInvoke(prm)
	if err != nil {
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

	// Client may not return errors if the table is missing, so check this case additionally.
	// The absence of a signature in the response can be taken as an eACL absence criterion,
	// since unsigned table cannot be approved in the storage by design.
	if len(sig) == 0 {
		return nil, container.ErrEACLNotFound
	}

	pub, err := client.BytesFromStackItem(arr[2])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of eACL public key (%s): %w", eaclMethod, err)
	}

	binToken, err := client.BytesFromStackItem(arr[3])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of eACL session token (%s): %w", eaclMethod, err)
	}

	table := eacl.NewTable()
	if err = table.Unmarshal(rawEACL); err != nil {
		// use other major version if there any
		return nil, err
	}

	if len(binToken) > 0 {
		tok := session.NewToken()

		err = tok.Unmarshal(binToken)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal session token: %w", err)
		}

		table.SetSessionToken(tok)
	}

	tableSignature := signature.New()
	tableSignature.SetKey(pub)
	tableSignature.SetSign(sig)

	table.SetSignature(tableSignature)

	return table, nil
}
