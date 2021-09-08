package event

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
)

// NotaryType is a notary event enumeration type.
type NotaryType string

// NotaryEvent is an interface that is
// provided by Neo:Morph notary event
// structures.
type NotaryEvent interface {
	ScriptHash() util.Uint160
	Type() NotaryType
	Params() []Op

	Raw() *payload.P2PNotaryRequest
}

// Equal compares two NotaryType values and
// returns true if they are equal.
func (t NotaryType) Equal(t2 NotaryType) bool {
	return string(t) == string(t2)
}

// String returns casted to string NotaryType.
func (t NotaryType) String() string {
	return string(t)
}

// NotaryTypeFromBytes converts bytes slice to NotaryType.
func NotaryTypeFromBytes(data []byte) NotaryType {
	return NotaryType(data)
}

// NotaryTypeFromString converts string to NotaryType.
func NotaryTypeFromString(str string) NotaryType {
	return NotaryType(str)
}

// UnexpectedArgNumErr returns error when notary parsers
// get unexpected amount of argument in contract call.
func UnexpectedArgNumErr(method string) error {
	return fmt.Errorf("unexpected arguments amount in %s call", method)
}

// UnexpectedOpcode returns error when notary parsers
// get unexpected opcode in contract call.
func UnexpectedOpcode(method string, op opcode.Opcode) error {
	return fmt.Errorf("unexpected opcode in %s call: %s", method, op)
}
