package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

const (
	putArgCnt      = 4
	putNamedArgCnt = 6 // `putNamed` has the same args as `put` + (name, zone) (2)
)

func (p *Put) setRawContainer(v []byte) {
	p.rawContainer = v
}

func (p *Put) setSignature(v []byte) {
	p.signature = v
}

func (p *Put) setPublicKey(v []byte) {
	p.publicKey = v
}

func (p *Put) setToken(v []byte) {
	p.token = v
}

func (p *Put) setName(v string) {
	p.name = v
}

func (p *Put) setZone(v string) {
	p.zone = v
}

func (p *Put) setMetaOnChain(v bool) {
	p.metaOnChain = v
}

var putFieldSetters = []func(*Put, []byte){
	(*Put).setRawContainer,
	(*Put).setSignature,
	(*Put).setPublicKey,
	(*Put).setToken,
}

const (
	// PutNotaryEvent is method name for container put operations
	// in `Container` contract. Is used as identificator for notary
	// put container requests.
	PutNotaryEvent = "put"

	// PutNamedNotaryEvent is an ID of notary "put named container" notification.
	PutNamedNotaryEvent = "putNamed"
)

func parsePutNotary(ev *Put, raw *payload.P2PNotaryRequest, args []scparser.PushedItem, t event.NotaryType) error {
	switch l := len(args); l {
	case putArgCnt + 3:
		err := parseNamedArgs(ev, args[putArgCnt:])
		if err != nil {
			return err
		}
		enableMeta, err := event.GetValueFromArg(args, l-1, t.String(), scparser.GetBoolFromInstr)
		if err != nil {
			return err
		}
		ev.setMetaOnChain(enableMeta)
	case putArgCnt + 2:
		err := parseNamedArgs(ev, args[putArgCnt:])
		if err != nil {
			return err
		}
	case putArgCnt:
	default:
		return fmt.Errorf("%s: unknown number of args: %d", t, l)
	}

	for i := range args[:putArgCnt] {
		b, err := event.GetValueFromArg(args, i, t.String(), scparser.GetBytesFromInstr)
		if err != nil {
			return err
		}
		putFieldSetters[i](ev, b)
	}

	ev.notaryRequest = raw

	return nil
}

func parseNamedArgs(ev *Put, args []scparser.PushedItem) error {
	name, err := scparser.GetStringFromInstr(args[0].Instruction)
	if err != nil {
		return fmt.Errorf("zone: %w", err)
	}

	zone, err := scparser.GetStringFromInstr(args[1].Instruction)
	if err != nil {
		return fmt.Errorf("name: %w", err)
	}

	ev.setName(name)
	ev.setZone(zone)

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
