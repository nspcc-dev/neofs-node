package event

import "github.com/nspcc-dev/neo-go/pkg/util"

type scriptHashValue struct {
	hash util.Uint160
}

type typeValue struct {
	typ Type
}

type scriptHashWithType struct {
	scriptHashValue
	typeValue
}

// SetScriptHash is a script hash setter.
func (s *scriptHashValue) SetScriptHash(v util.Uint160) {
	s.hash = v
}

func (s scriptHashValue) scriptHash() util.Uint160 {
	return s.hash
}

// SetType is an event type setter.
func (s *typeValue) SetType(v Type) {
	s.typ = v
}

func (s typeValue) getType() Type {
	return s.typ
}
