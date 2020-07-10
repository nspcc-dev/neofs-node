package event

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/pkg/errors"
)

// Parser is a function that constructs Event
// from the StackItem list.
type Parser func([]smartcontract.Parameter) (Event, error)

// ParserInfo is a structure that groups
// the parameters of particular contract
// notification event parser.
type ParserInfo struct {
	scriptHashWithType

	p Parser
}

type wrongPrmNumber struct {
	exp, act int
}

// WrongNumberOfParameters returns an error about wrong number of smart contract parameters.
func WrongNumberOfParameters(exp, act int) error {
	return &wrongPrmNumber{
		exp: exp,
		act: act,
	}
}

func (s wrongPrmNumber) Error() string {
	return errors.Errorf("wrong parameter count: expected %d, has %d", s.exp, s.act).Error()
}

// SetParser is an event parser setter.
func (s *ParserInfo) SetParser(v Parser) {
	s.p = v
}

func (s ParserInfo) parser() Parser {
	return s.p
}

// SetType is an event type setter.
func (s *ParserInfo) SetType(v Type) {
	s.typ = v
}

func (s ParserInfo) getType() Type {
	return s.typ
}
