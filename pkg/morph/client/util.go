package client

import (
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/pkg/errors"
)

/*
   Use these function to parse stack parameters obtained from `TestInvoke`
   function to native go types. You should know upfront return types of invoked
   method.
*/

// BoolFromStackItem receives boolean value from the value of a smart contract parameter.
func BoolFromStackItem(param stackitem.Item) (bool, error) {
	switch param.Type() {
	case stackitem.BooleanT, stackitem.IntegerT, stackitem.ByteArrayT:
		return param.TryBool()
	default:
		return false, errors.Errorf("chain/client: %s is not a bool type", param.Type())
	}
}

// IntFromStackItem receives numerical value from the value of a smart contract parameter.
func IntFromStackItem(param stackitem.Item) (int64, error) {
	switch param.Type() {
	case stackitem.IntegerT, stackitem.ByteArrayT:
		i, err := param.TryInteger()
		if err != nil {
			return 0, err
		}

		return i.Int64(), nil
	default:
		return 0, errors.Errorf("chain/client: %s is not an integer type", param.Type())
	}
}

// BigIntFromStackItem receives numerical value from the value of a smart contract parameter.
func BigIntFromStackItem(param stackitem.Item) (*big.Int, error) {
	return param.TryInteger()
}

// BytesFromStackItem receives binary value from the value of a smart contract parameter.
func BytesFromStackItem(param stackitem.Item) ([]byte, error) {
	switch param.Type() {
	case stackitem.BufferT, stackitem.ByteArrayT:
		return param.TryBytes()
	case stackitem.IntegerT:
		n, err := param.TryInteger()
		if err != nil {
			return nil, errors.Wrap(err, "can't parse integer bytes")
		}

		return n.Bytes(), nil
	case stackitem.AnyT:
		if param.Value() == nil {
			return nil, nil
		}
		fallthrough
	default:
		return nil, errors.Errorf("chain/client: %s is not a byte array type", param.Type())
	}
}

// ArrayFromStackItem returns the slice contract parameters from passed parameter.
//
// If passed parameter carries boolean false value, (nil, nil) returns.
func ArrayFromStackItem(param stackitem.Item) ([]stackitem.Item, error) {
	switch param.Type() {
	case stackitem.AnyT:
		return nil, nil
	case stackitem.ArrayT, stackitem.StructT:
		items, ok := param.Value().([]stackitem.Item)
		if !ok {
			return nil, errors.Errorf("chain/client: can't convert %T to parameter slice", param.Value())
		}

		return items, nil
	default:
		return nil, errors.Errorf("chain/client: %s is not an array type", param.Type())
	}
}

// StringFromStackItem receives string value from the value of a smart contract parameter.
func StringFromStackItem(param stackitem.Item) (string, error) {
	if param.Type() != stackitem.ByteArrayT {
		return "", errors.Errorf("chain/client: %s is not an integer type", param.Type())
	}

	return stackitem.ToString(param)
}
