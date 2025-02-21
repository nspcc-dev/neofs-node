package container

import (
	"crypto/sha256"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// AnnounceLoadPrm groups parameters of AnnounceLoad operation.
type AnnounceLoadPrm struct {
	a   container.SizeEstimation
	key []byte

	client.InvokePrmOptional
}

// SetAnnouncement sets announcement.
func (a2 *AnnounceLoadPrm) SetAnnouncement(a container.SizeEstimation) {
	a2.a = a
}

// SetReporter sets public key of the reporter.
func (a2 *AnnounceLoadPrm) SetReporter(key []byte) {
	a2.key = key
}

// AnnounceLoad saves container size estimation calculated by storage node
// with key in NeoFS system through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) AnnounceLoad(p AnnounceLoadPrm) error {
	cnr := p.a.Container()

	prm := client.InvokePrm{}
	prm.SetMethod(putSizeMethod)
	prm.SetArgs(p.a.Epoch(), cnr[:], p.a.Value(), p.key)
	prm.InvokePrmOptional = p.InvokePrmOptional

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", putSizeMethod, err)
	}
	return nil
}

// ListLoadEstimationsByEpoch returns a list of container load estimations for to the specified epoch.
// The list is composed through Container contract call.
func (c *Client) ListLoadEstimationsByEpoch(epoch uint64) (map[cid.ID]*Estimations, error) {
	// neo-go's stack elements default limit
	// is 2048, make it less a little
	const prefetchNumber = 2000

	kvs, err := c.client.TestInvokeIterator(listSizesMethod, prefetchNumber, epoch)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listSizesMethod, err)
	}

	resMap := make(map[cid.ID]*Estimations)
	for i := range kvs {
		kv, err := client.ArrayFromStackItem(kvs[i])
		if err != nil {
			return nil, fmt.Errorf("could not unwrap iterator key-value pair: %w", err)
		}

		if len(kv) != 2 { // key-value array
			return nil, fmt.Errorf("incorrect contract key-value struct size: %d", len(kv))
		}

		cnr, err := cidFromStorageKey(kv[0])
		if err != nil {
			return nil, fmt.Errorf("storage key handling: %w", err)
		}

		var e Estimation
		err = e.FromStackItem(kv[1])
		if err != nil {
			return nil, fmt.Errorf("storage value handling: %w", err)
		}

		if ee, found := resMap[cnr]; found {
			ee.Values = append(ee.Values, e)
		} else {
			resMap[cnr] = &Estimations{
				ContainerID: cnr,
				Values:      []Estimation{e},
			}
		}
	}

	return resMap, nil
}

func cidFromStorageKey(stack stackitem.Item) (cid.ID, error) {
	var cnr cid.ID

	k, err := client.BytesFromStackItem(stack)
	if err != nil {
		return cnr, fmt.Errorf("incorrect estimation key from iterator: %w", err)
	}

	if len(k) < sha256.Size {
		return cnr, fmt.Errorf("incorrect estimations key len: %d", len(k))
	}

	err = cnr.Decode(k[:sha256.Size])
	if err != nil {
		return cnr, fmt.Errorf("cid decoding: %w", err)
	}

	return cnr, nil
}

// Estimation is a structure of single container load estimation
// reported by storage node.
type Estimation struct {
	Size uint64

	Reporter []byte
}

// ToStackItem implements stackitem.Convertible.
func (e *Estimation) ToStackItem() (stackitem.Item, error) {
	panic("not for now")
}

// FromStackItem implements stackitem.Convertible.
func (e *Estimation) FromStackItem(item stackitem.Item) error {
	v, err := client.ArrayFromStackItem(item)
	if err != nil {
		return fmt.Errorf("incorrect estimation value from iterator: %w", err)
	}

	if len(v) != 2 { // 2 field resulting struct
		return fmt.Errorf("incorrect estimation struct size: %d", len(v))
	}

	pk, err := client.BytesFromStackItem(v[0])
	if err != nil {
		return fmt.Errorf("incorrect reporter key %w", err)
	}

	size, err := client.IntFromStackItem(v[1])
	if err != nil {
		return fmt.Errorf("incorrect container size: %w", err)
	}

	e.Reporter = pk
	e.Size = uint64(size)

	return nil
}

// Estimations is a structure of grouped container load estimation inside Container contract.
type Estimations struct {
	ContainerID cid.ID

	Values []Estimation
}
