package client

import (
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
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
		return false, fmt.Errorf("chain/client: %s is not a bool type", param.Type())
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
		return 0, fmt.Errorf("chain/client: %s is not an integer type", param.Type())
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
			return nil, fmt.Errorf("can't parse integer bytes: %w", err)
		}

		return n.Bytes(), nil
	case stackitem.AnyT:
		if param.Value() == nil {
			return nil, nil
		}
		fallthrough
	default:
		return nil, fmt.Errorf("chain/client: %s is not a byte array type", param.Type())
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
			return nil, fmt.Errorf("chain/client: can't convert %T to parameter slice", param.Value())
		}

		return items, nil
	default:
		return nil, fmt.Errorf("chain/client: %s is not an array type", param.Type())
	}
}

// StringFromStackItem receives string value from the value of a smart contract parameter.
func StringFromStackItem(param stackitem.Item) (string, error) {
	if param.Type() != stackitem.ByteArrayT {
		return "", fmt.Errorf("chain/client: %s is not an string type", param.Type())
	}

	return stackitem.ToString(param)
}

// MapFromStackItem returns a map parsed from contract parameter.
func MapFromStackItem(param stackitem.Item) ([]stackitem.MapElement, error) {
	switch typ := param.Type(); typ {
	case stackitem.MapT:
		mm, ok := param.Value().([]stackitem.MapElement)
		if !ok {
			return nil, fmt.Errorf("chain/client: can't convert %T to slice of map elements", param.Value())
		}

		return mm, nil
	default:
		return nil, fmt.Errorf("chain/client: %s is not a map type", typ)
	}
}

func addFeeCheckerModifier(add int64) func(r *result.Invoke, t *transaction.Transaction) error {
	return func(r *result.Invoke, t *transaction.Transaction) error {
		if r.State != HaltState {
			return &notHaltStateError{state: r.State, exception: r.FaultException}
		}

		if add == 0 {
			add = t.SystemFee / 10
		}
		t.SystemFee += add

		return nil
	}
}
