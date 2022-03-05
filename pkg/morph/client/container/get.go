package container

import (
	"fmt"
	"strings"

	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/signature"
)

type containerSource Client

func (x *containerSource) Get(cid *cid.ID) (*container.Container, error) {
	return Get((*Client)(x), cid)
}

// AsContainerSource provides container Source interface
// from Wrapper instance.
func AsContainerSource(w *Client) core.Source {
	return (*containerSource)(w)
}

// Get marshals container ID, and passes it to Wrapper's Get method.
//
// Returns error if cid is nil.
func Get(c *Client, cid *cid.ID) (*container.Container, error) {
	return c.Get(cid.ToV2().GetValue())
}

// Get reads the container from NeoFS system by binary identifier
// through Container contract call.
//
// If an empty slice is returned for the requested identifier,
// storage.ErrNotFound error is returned.
func (c *Client) Get(cid []byte) (*container.Container, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(getMethod)
	prm.SetArgs(cid)

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		// TODO(fyrchik): reuse messages from container contract.
		// Currently there are some dependency problems:
		// github.com/nspcc-dev/neofs-node/pkg/innerring imports
		//        github.com/nspcc-dev/neofs-sdk-go/audit imports
		//        github.com/nspcc-dev/neofs-api-go/v2/audit: ambiguous import: found package github.com/nspcc-dev/neofs-api-go/v2/audit in multiple modules:
		//        github.com/nspcc-dev/neofs-api-go v1.27.1 (/home/dzeta/go/pkg/mod/github.com/nspcc-dev/neofs-api-go@v1.27.1/v2/audit)
		//        github.com/nspcc-dev/neofs-api-go/v2 v2.11.0-pre.0.20211201134523-3604d96f3fe1 (/home/dzeta/go/pkg/mod/github.com/nspcc-dev/neofs-api-go/v2@v2.11.0-pre.0.20211201134523-3604d96f3fe1/audit)
		if strings.Contains(err.Error(), "container does not exist") {
			return nil, core.ErrNotFound
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

	cnr := container.New()
	if err := cnr.Unmarshal(cnrBytes); err != nil {
		// use other major version if there any
		return nil, fmt.Errorf("can't unmarshal container: %w", err)
	}

	if len(tokBytes) > 0 {
		tok := session.NewToken()

		err = tok.Unmarshal(tokBytes)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal session token: %w", err)
		}

		cnr.SetSessionToken(tok)
	}

	sig := signature.New()
	sig.SetKey(pub)
	sig.SetSign(sigBytes)
	cnr.SetSignature(sig)

	return cnr, nil
}
