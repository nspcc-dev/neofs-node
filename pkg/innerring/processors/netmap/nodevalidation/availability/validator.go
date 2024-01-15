package availability

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Validator is a utility that verifies node's
// accessibility according to its announced addresses.
//
// For correct operation, the Validator must be created
// using the constructor (New). After successful creation,
// the Validator is immediately ready to work through API.
type Validator struct{}

func (v Validator) Verify(nodeInfo netmap.NodeInfo) error {
	var results []*client.ResEndpointInfo
	var err error

	nodeInfo.IterateNetworkEndpoints(func(s string) bool {
		var res *client.ResEndpointInfo
		var c *client.Client

		c, err = createSDKClient(s)
		if err != nil {
			err = fmt.Errorf("'%s': client creation: %w", s, err)
			return true
		}
		defer func() {
			_ = c.Close()
		}()

		timeoutContext, cancel := context.WithTimeout(context.Background(), pingTimeout)
		defer cancel()

		res, err = c.EndpointInfo(timeoutContext, client.PrmEndpointInfo{})
		if err != nil {
			err = fmt.Errorf("'%s': could not ping node with `EndpointInfo`: %w", s, err)
			return true
		}

		results = append(results, res)
		return false
	})
	if err != nil {
		return err
	}

	for _, res := range results {
		err = compareNodeInfos(nodeInfo, res.NodeInfo())
		if err != nil {
			return fmt.Errorf("`EndpointInfo` RPC call result differs: %w", err)
		}
	}

	return nil
}

// New creates a new instance of the Validator.
//
// Panics if at least one value of the parameters is invalid.
//
// The created Validator does not require additional
// initialization and is completely ready for work.
func New() *Validator {
	return &Validator{}
}

func compareNodeInfos(niExp, niGot netmap.NodeInfo) error {
	// a node can be in a STATE_1 (and respond with it)
	// but the request can mean a state transfer to a
	// STATE_2, so make both node infos in the same state,
	// e.g. ONLINE
	niGot.SetOnline()
	niExp.SetOnline()
	if exp, got := niExp.Marshal(), niGot.Marshal(); bytes.Equal(exp, got) {
		return nil
	}

	var err error

	if exp, got := niExp.PublicKey(), niGot.PublicKey(); !bytes.Equal(exp, got) {
		return fmt.Errorf("public key: got %x, expect %x", got, exp)
	}

	if exp, got := niExp.NumberOfAttributes(), niGot.NumberOfAttributes(); exp != got {
		return fmt.Errorf("attr number: got %d, expect %d", got, exp)
	}

	niExp.IterateAttributes(func(key, value string) {
		vGot := niGot.Attribute(key)
		if vGot != value {
			err = fmt.Errorf("non-equal %s attribute: got %s, expect %s", key, vGot, value)
		}
	})
	if err != nil {
		return err
	}

	if exp, got := niExp.NumberOfNetworkEndpoints(), niGot.NumberOfNetworkEndpoints(); exp != got {
		return fmt.Errorf("address number: got %d, expect %d", got, exp)
	}

	expAddrM := make(map[string]struct{}, niExp.NumberOfAttributes())
	niExp.IterateNetworkEndpoints(func(s string) bool {
		expAddrM[s] = struct{}{}
		return false
	})

	niGot.IterateNetworkEndpoints(func(s string) bool {
		if _, ok := expAddrM[s]; !ok {
			err = fmt.Errorf("got unexpected address: %s", s)
			return true
		}

		return false
	})
	if err != nil {
		return err
	}

	return nil
}

const pingTimeout = 15 * time.Second

func createSDKClient(e string) (*client.Client, error) {
	var a network.Address
	err := a.FromString(e)
	if err != nil {
		return nil, fmt.Errorf("parsing address: %w", err)
	}

	var prmInit client.PrmInit
	var prmDial client.PrmDial

	prmDial.SetTimeout(pingTimeout)
	prmDial.SetStreamTimeout(pingTimeout)
	prmDial.SetServerURI(a.URIAddr())

	c, err := client.New(prmInit)
	if err != nil {
		return nil, fmt.Errorf("can't create SDK client: %w", err)
	}

	err = c.Dial(prmDial)
	if err != nil {
		return nil, fmt.Errorf("can't init SDK client: %w", err)
	}

	return c, nil
}
