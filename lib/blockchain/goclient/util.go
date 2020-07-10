package goclient

import (
	"encoding/binary"

	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/pkg/errors"
)

/*
   Use these function to parse stack parameters obtained from `TestInvoke`
   function to native go types. You should know upfront return types of invoked
   method.
*/

// BoolFromStackParameter receives boolean value from the value of a smart contract parameter.
func BoolFromStackParameter(param sc.Parameter) (bool, error) {
	switch param.Type {
	case sc.BoolType:
		val, ok := param.Value.(bool)
		if !ok {
			return false, errors.Errorf("chain/client: can't convert %T to boolean", param.Value)
		}

		return val, nil
	case sc.IntegerType:
		val, ok := param.Value.(int64)
		if !ok {
			return false, errors.Errorf("chain/client: can't convert %T to boolean", param.Value)
		}

		return val > 0, nil
	case sc.ByteArrayType:
		val, ok := param.Value.([]byte)
		if !ok {
			return false, errors.Errorf("chain/client: can't convert %T to boolean", param.Value)
		}

		return len(val) != 0, nil
	default:
		return false, errors.Errorf("chain/client: %s is not a bool type", param.Type)
	}
}

// IntFromStackParameter receives numerical value from the value of a smart contract parameter.
func IntFromStackParameter(param sc.Parameter) (int64, error) {
	switch param.Type {
	case sc.IntegerType:
		val, ok := param.Value.(int64)
		if !ok {
			return 0, errors.Errorf("chain/client: can't convert %T to integer", param.Value)
		}

		return val, nil
	case sc.ByteArrayType:
		val, ok := param.Value.([]byte)
		if !ok || len(val) > 8 {
			return 0, errors.Errorf("chain/client: can't convert %T to integer", param.Value)
		}

		res := make([]byte, 8)
		copy(res[:len(val)], val)

		return int64(binary.LittleEndian.Uint64(res)), nil
	default:
		return 0, errors.Errorf("chain/client: %s is not an integer type", param.Type)
	}
}

// BytesFromStackParameter receives binary value from the value of a smart contract parameter.
func BytesFromStackParameter(param sc.Parameter) ([]byte, error) {
	if param.Type != sc.ByteArrayType {
		return nil, errors.Errorf("chain/client: %s is not a byte array type", param.Type)
	}

	val, ok := param.Value.([]byte)
	if !ok {
		return nil, errors.Errorf("chain/client: can't convert %T to byte slice", param.Value)
	}

	return val, nil
}

// ArrayFromStackParameter returns the slice contract parameters from passed parameter.
//
// If passed parameter carries boolean false value, (nil, nil) returns.
func ArrayFromStackParameter(param sc.Parameter) ([]sc.Parameter, error) {
	if param.Type == sc.BoolType && !param.Value.(bool) {
		return nil, nil
	}

	if param.Type != sc.ArrayType {
		return nil, errors.Errorf("chain/client: %s is not an array type", param.Type)
	}

	val, ok := param.Value.([]sc.Parameter)
	if !ok {
		return nil, errors.Errorf("chain/client: can't convert %T to parameter slice", param.Value)
	}

	return val, nil
}

// StringFromStackParameter receives string value from the value of a smart contract parameter.
func StringFromStackParameter(param sc.Parameter) (string, error) {
	switch param.Type {
	case sc.StringType:
		val, ok := param.Value.(string)
		if !ok {
			return "", errors.Errorf("chain/client: can't convert %T to string", param.Value)
		}

		return val, nil
	case sc.ByteArrayType:
		val, ok := param.Value.([]byte)
		if !ok {
			return "", errors.Errorf("chain/client: can't convert %T to string", param.Value)
		}

		return string(val), nil
	default:
		return "", errors.Errorf("chain/client: %s is not a string type", param.Type)
	}
}

// ReadStorage of the contract directly. Use it for debug, try to obtain
// smart-contract data from contract method with TestInvoke function.
func ReadStorage(c *Client, contract util.Uint160, key []byte) ([]byte, error) {
	return c.cli.GetStorageByHash(contract, key)
}
