package container

import (
	"crypto/sha256"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// ListLoadEstimationsByEpoch returns a list of container load estimations for to the specified epoch.
// The list is composed through Container contract call.
func (c *Client) ListLoadEstimationsByEpoch(epoch uint64) (map[cid.ID]*Estimations, error) {
	kvs, err := c.client.TestInvokeIterator(listSizesMethod, iteratorPrefetchNumber, epoch)
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
