package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	// PutNotaryEvent is method name for container put operations
	// in `Container` contract. Is used as identificator for notary
	// put container requests.
	PutNotaryEvent = "put"

	// PutNamedNotaryEvent is an ID of notary "put named container" notification.
	PutNamedNotaryEvent = "putNamed"
)

const (
	putArgCnt      = 4
	putNamedArgCnt = 6 // `putNamed` has the same args as `put` + (name, zone) (2)
)

func parsePutNotary(ev *Put, raw *payload.P2PNotaryRequest, args []scparser.PushedItem, t event.NotaryType) error {
	switch l := len(args); l {
	case putArgCnt + 3:
		err := parseNamedArgs(ev, args[putArgCnt:])
		if err != nil {
			return err
		}
		ev.metaOnChain, err = event.GetValueFromArg(args, l-1, t.String(), scparser.GetBoolFromInstr)
		if err != nil {
			return err
		}
	case putArgCnt + 2:
		err := parseNamedArgs(ev, args[putArgCnt:])
		if err != nil {
			return err
		}
	case putArgCnt:
	default:
		return fmt.Errorf("%s: unknown number of args: %d", t, l)
	}

	var err error
	ev.rawContainer, err = event.GetValueFromArg(args, 0, t.String(), scparser.GetBytesFromInstr)
	if err != nil {
		return err
	}
	ev.signature, err = event.GetValueFromArg(args, 1, t.String(), scparser.GetBytesFromInstr)
	if err != nil {
		return err
	}
	ev.publicKey, err = event.GetValueFromArg(args, 2, t.String(), scparser.GetBytesFromInstr)
	if err != nil {
		return err
	}
	ev.token, err = event.GetValueFromArg(args, 3, t.String(), scparser.GetBytesFromInstr)
	if err != nil {
		return err
	}

	ev.notaryRequest = raw

	return nil
}

func parseNamedArgs(ev *Put, args []scparser.PushedItem) error {
	var err error
	ev.name, err = scparser.GetStringFromInstr(args[0].Instruction)
	if err != nil {
		return fmt.Errorf("zone: %w", err)
	}

	ev.zone, err = scparser.GetStringFromInstr(args[1].Instruction)
	if err != nil {
		return fmt.Errorf("name: %w", err)
	}

	return nil
}

// ParsePutNotary from NotaryEvent into container event structure.
func ParsePutNotary(ne event.NotaryEvent) (event.Event, error) {
	var ev Put

	err := parsePutNotary(&ev, ne.Raw(), ne.Params(), ne.Type())
	if err != nil {
		return nil, err
	}

	return ev, nil
}

// ParsePutNamedNotary parses PutNamed event structure from generic event.NotaryEvent.
func ParsePutNamedNotary(ne event.NotaryEvent) (event.Event, error) {
	args, err := event.GetArgs(ne, putNamedArgCnt)
	if err != nil {
		return nil, err
	}

	var ev Put
	err = parsePutNotary(&ev, ne.Raw(), args, ne.Type())
	if err != nil {
		return nil, err
	}

	return ev, nil
}
