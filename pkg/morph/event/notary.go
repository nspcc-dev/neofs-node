package event

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// NotaryType is a notary event enumeration type.
type NotaryType string

// NotaryEvent is an interface that is
// provided by Neo:Morph notary event
// structures.
type NotaryEvent interface {
	ScriptHash() util.Uint160
	Type() NotaryType
	Params() []scparser.PushedItem

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

// NotaryTypeFromString converts string to NotaryType.
func NotaryTypeFromString(str string) NotaryType {
	return NotaryType(str)
}
