package event

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
)

// NotificationParser is a function that constructs Event
// from the StackItem list.
type NotificationParser func(*subscriptions.NotificationEvent) (Event, error)

// NotificationParserInfo is a structure that groups
// the parameters of particular contract
// notification event parser.
type NotificationParserInfo struct {
	scriptHashWithType

	p NotificationParser
}

// NotaryPreparator constructs NotaryEvent
// from the NotaryRequest event.
type NotaryPreparator interface {
	Prepare(*payload.P2PNotaryRequest) (NotaryEvent, error)
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
