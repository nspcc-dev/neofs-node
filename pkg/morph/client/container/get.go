package container

import (
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

type containerSource Client

func (x *containerSource) Get(cnr cid.ID) (container.Container, error) {
	return Get((*Client)(x), cnr)
}

// AsContainerSource provides container Source interface
// from Wrapper instance.
func AsContainerSource(w *Client) containercore.Source {
	return (*containerSource)(w)
}

// Get marshals container ID, and passes it to Wrapper's Get method.
func Get(c *Client, cnr cid.ID) (container.Container, error) {
	return c.Get(cnr[:])
}

// Get reads the container from NeoFS system by binary identifier
// through Container contract call.
//
// If an empty slice is returned for the requested identifier,
// storage.ErrNotFound error is returned.
func (c *Client) Get(cid []byte) (container.Container, error) {
	var cnr container.Container
	prm := client.TestInvokePrm{}
	method := getInfoMethod
	prm.SetMethod(method)
	prm.SetArgs(cid)

	arr, err := c.client.TestInvoke(prm)
	if err != nil && isMethodNotFoundError(err, method) {
		method = getDataMethod
		prm.SetMethod(method)
		arr, err = c.client.TestInvoke(prm)
		if err != nil && isMethodNotFoundError(err, method) {
			method = getMethod
			prm.SetMethod(method)
			arr, err = c.client.TestInvoke(prm)
		}
	}
	if err != nil {
		if strings.Contains(err.Error(), containerrpc.NotFoundError) {
			var errNotFound apistatus.ContainerNotFound

			return cnr, errNotFound
		}
		return cnr, fmt.Errorf("could not perform test invocation (%s): %w", method, err)
	} else if ln := len(arr); ln != 1 {
		return cnr, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}

	if method != getInfoMethod {
		return decodeOldGetResponse(arr, method)
	}

	cnr, err = containerFromStackItem(arr[0])
	if err != nil {
		return cnr, fmt.Errorf("invalid %q method result: invalid stack item: %w", method, err)
	}

	return cnr, nil
}

func decodeOldGetResponse(arr []stackitem.Item, method string) (container.Container, error) {
	var cnr container.Container

	if method == getMethod {
		var err error
		arr, err = client.ArrayFromStackItem(arr[0])
		if err != nil {
			return cnr, fmt.Errorf("could not get item array of container (%s): %w", getMethod, err)
		}

		if len(arr) == 0 {
			return cnr, fmt.Errorf("unexpected container stack item count (%s): %d", getMethod, len(arr))
		}
	}

	cnrBytes, err := client.BytesFromStackItem(arr[0])
	if err != nil {
		return cnr, fmt.Errorf("could not get byte array of container (%s): %w", method, err)
	}

	if err := cnr.Unmarshal(cnrBytes); err != nil {
		// use other major version if there any
		return cnr, fmt.Errorf("can't unmarshal container: %w", err)
	}

	return cnr, nil
}
