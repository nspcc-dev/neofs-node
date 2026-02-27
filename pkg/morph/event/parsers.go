package event

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
)

// NotificationParser is a function that constructs Event
// from the StackItem list.
type NotificationParser func(*state.ContainedNotificationEvent) (Event, error)

// NotificationParserInfo is a structure that groups
// the parameters of particular contract
// notification event parser.
type NotificationParserInfo struct {
	scriptHashWithType

	p NotificationParser
}

// NotaryParser is a function that constructs Event
// from the NotaryEvent event.
type NotaryParser func(NotaryEvent) (Event, error)

// NotaryParserInfo is a structure that groups
// the parameters of particular notary request
// event parser.
type NotaryParserInfo struct {
	notaryRequestTypes

	p NotaryParser
}

func (n *NotaryParserInfo) parser() NotaryParser {
	return n.p
}

func (n *NotaryParserInfo) SetParser(p NotaryParser) {
	n.p = p
}

// SetParser is an event parser setter.
func (s *NotificationParserInfo) SetParser(v NotificationParser) {
	s.p = v
}

func (s NotificationParserInfo) parser() NotificationParser {
	return s.p
}

// SetType is an event type setter.
func (s *NotificationParserInfo) SetType(v Type) {
	s.typ = v
}

func (s NotificationParserInfo) getType() Type {
	return s.typ
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
	return fmt.Errorf("wrong parameter count: expected %d, has %d", s.exp, s.act).Error()
}

// GetArgs returns a list of contract call arguments from the specified
// NotaryEvent ensuring the number of arguments matches the expectations.
func GetArgs(e NotaryEvent, expectedNum int) ([]scparser.PushedItem, error) {
	args := e.Params()
	if len(args) != expectedNum {
		return nil, WrongNumberOfParameters(expectedNum, len(args))
	}
	return args, nil
}

// GetValueFromArg parses a contract call argument with the specified index from
// the list of arguments given the parsing function.
func GetValueFromArg[T any](args []scparser.PushedItem, i int, desc string, f scparser.GetEFromInstr[T]) (v T, err error) {
	v, err = f(args[i].Instruction)
	if err != nil {
		return v, WrapInvalidArgError(i, desc, err)
	}
	return v, nil
}

// WrapInvalidArgError wraps an error with the specified argument index and
// description.
func WrapInvalidArgError(i int, desc string, err error) error {
	return fmt.Errorf("argument #%d (%s): %w", i, desc, err)
}
