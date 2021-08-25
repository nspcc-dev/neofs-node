package event

import "github.com/nspcc-dev/neo-go/pkg/vm/opcode"

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
