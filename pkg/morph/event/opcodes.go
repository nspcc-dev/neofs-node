package event

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/bigint"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
)

// Op is wrapper over Neo VM's opcode
// and its parameter.
type Op struct {
	code  opcode.Opcode
	param []byte
}

// Code returns Neo VM opcode.
func (o Op) Code() opcode.Opcode {
	return o.code
}

// Param returns parameter of wrapped
// Neo VM opcode.
func (o Op) Param() []byte {
	return o.param
}

// BytesFromOpcode tries to retrieve bytes from Op.
func BytesFromOpcode(op Op) ([]byte, error) {
	switch code := op.Code(); code {
	case opcode.PUSHDATA1, opcode.PUSHDATA2, opcode.PUSHDATA4:
		return op.Param(), nil
	default:
		return nil, fmt.Errorf("unexpected ByteArray opcode %s", code)
	}
}

// IntFromOpcode tries to retrieve bytes from Op.
func IntFromOpcode(op Op) (int64, error) {
	switch code := op.Code(); {
	case code == opcode.PUSHM1:
		return -1, nil
	case code >= opcode.PUSH0 && code <= opcode.PUSH16:
		return int64(code - opcode.PUSH0), nil
	case code <= opcode.PUSHINT256:
		return bigint.FromBytes(op.Param()).Int64(), nil
	default:
		return 0, fmt.Errorf("unexpected INT opcode %s", code)
	}
}
