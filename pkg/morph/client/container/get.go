package container

import (
	"fmt"
	"strings"

	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
)

type containerSource Client

func (x *containerSource) Get(cnr cid.ID) (*containercore.Container, error) {
	return Get((*Client)(x), cnr)
}

// AsContainerSource provides container Source interface
// from Wrapper instance.
func AsContainerSource(w *Client) containercore.Source {
	return (*containerSource)(w)
}

// Get marshals container ID, and passes it to Wrapper's Get method.
func Get(c *Client, cnr cid.ID) (*containercore.Container, error) {
	return c.Get(cnr[:])
}

// Get reads the container from NeoFS system by binary identifier
// through Container contract call.
//
// If an empty slice is returned for the requested identifier,
// storage.ErrNotFound error is returned.
func (c *Client) Get(cid []byte) (*containercore.Container, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(getMethod)
	prm.SetArgs(cid)

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			var errNotFound apistatus.ContainerNotFound

			return nil, errNotFound
		}
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", getMethod, err)
	} else if ln := len(res); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", getMethod, ln)
	}

	arr, err := client.ArrayFromStackItem(res[0])
	if err != nil {
		return nil, fmt.Errorf("could not get item array of container (%s): %w", getMethod, err)
	}

	if len(arr) != 4 {
		return nil, fmt.Errorf("unexpected container stack item count (%s): %d", getMethod, len(arr))
	}

	cnrBytes, err := client.BytesFromStackItem(arr[0])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of container (%s): %w", getMethod, err)
	}

	sigBytes, err := client.BytesFromStackItem(arr[1])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of container signature (%s): %w", getMethod, err)
	}

	pub, err := client.BytesFromStackItem(arr[2])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of public key (%s): %w", getMethod, err)
	}

	tokBytes, err := client.BytesFromStackItem(arr[3])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of session token (%s): %w", getMethod, err)
	}

	var cnr containercore.Container

	if err := cnr.Value.Unmarshal(cnrBytes); err != nil {
		// use other major version if there any
		return nil, fmt.Errorf("can't unmarshal container: %w", err)
	}

	if len(tokBytes) > 0 {
		cnr.Session = new(session.Container)

		err = cnr.Session.Unmarshal(tokBytes)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal session token: %w", err)
		}
	}

	if len(pub) == 0 {
		return &cnr, nil
	}

	cnr.Signature, err = decodeSignature(pub, sigBytes)
	if err != nil {
		return nil, fmt.Errorf("decode signature: %w", err)
	}

	return &cnr, nil
}
