package blockchain

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// TODO: think how to merge functions with ones from client package (like client.ArrayFromStackItem)
//  maybe SDK?

// ToArray attempts to convert given stackitem.Item to the slice of
// stackitem.Item.
func ToArray(item stackitem.Item) ([]stackitem.Item, error) {
	if typ := item.Type(); typ != stackitem.ArrayT && typ != stackitem.StructT {
		return nil, fmt.Errorf("unexpected result stack item type %s", item.Type())
	}

	arr, ok := item.Value().([]stackitem.Item)
	if !ok {
		return nil, fmt.Errorf("failed to cast stack item of type %s to %T", item.Type(), arr)
	}

	return arr, nil
}

// ToArrayNonEmpty attempts to convert given stackitem.Item to the slice of
// stackitem.Item and returns provided error if result is empty.
func ToArrayNonEmpty(item stackitem.Item, onEmpty error) ([]stackitem.Item, error) {
	res, err := ToArray(item)
	if err == nil && len(res) == 0 {
		err = onEmpty
	}

	return res, err
}

// ToBytesNonEmpty attempts to convert given stackitem.Item to []byte
// and returns provided error if result is empty.
func ToBytesNonEmpty(item stackitem.Item, onEmpty error) ([]byte, error) {
	res, err := item.TryBytes()
	if err == nil && len(res) == 0 {
		err = onEmpty
	}

	return res, err
}
