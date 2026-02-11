package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectPut structure of container.SubmitObjectPut notification from FS chain.
type ObjectPut struct {
	cID cid.ID
	oID oid.ID

	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (ObjectPut) MorphEvent() {}

// ObjectID returns created object's ID.
func (op ObjectPut) ObjectID() oid.ID {
	return op.oID
}

// ContainerID returns created object's container ID.
func (op ObjectPut) ContainerID() cid.ID {
	return op.cID
}

// NotaryRequest returns raw notary request if notification
// was received via notary service.
func (op ObjectPut) NotaryRequest() *payload.P2PNotaryRequest {
	return op.notaryRequest
}

const (
	// ObjectPutNotaryEvent is method name for object submission operations
	// in `Container` contract. Is used as identificator for notary object
	// creation requests.
	ObjectPutNotaryEvent = "submitObjectPut"
)

// ParseObjectPut decodes notification event thrown by Container contract into
// ObjectPut and returns it as event.Event.
func ParseObjectPut(e event.NotaryEvent) (event.Event, error) {
	const expectedItemNumObjectPut = 2
	ev := ObjectPut{notaryRequest: e.Raw()}
	args := e.Params()
	if len(args) != expectedItemNumObjectPut {
		return nil, event.WrongNumberOfParameters(expectedItemNumObjectPut, len(args))
	}

	rawMap, err := scparser.GetBytesFromInstr(args[0].Instruction)
	if err != nil {
		return nil, fmt.Errorf("converting metadata information from AppCall parameter: %w", err)
	}
	m, err := stackitem.Deserialize(rawMap)
	if err != nil {
		return nil, fmt.Errorf("metadata deserialization: %w", err)
	}
	mm, err := client.MapFromStackItem(m)
	if err != nil {
		return nil, fmt.Errorf("parsing object meta data: %w", err)
	}

	for _, m := range mm {
		k, err := client.StringFromStackItem(m.Key)
		if err != nil {
			return nil, fmt.Errorf("reading metadata string key %T: %w", m.Key, err)
		}

		switch k {
		case "cid":
			rawCID, err := client.BytesFromStackItem(m.Value)
			if err != nil {
				return nil, fmt.Errorf("parsing container ID from stack item: %w", err)
			}
			err = ev.cID.Decode(rawCID)
			if err != nil {
				return nil, fmt.Errorf("decode container ID: %w", err)
			}
		case "oid":
			rawOID, err := client.BytesFromStackItem(m.Value)
			if err != nil {
				return nil, fmt.Errorf("parsing object ID from stack item: %w", err)
			}
			err = ev.oID.Decode(rawOID)
			if err != nil {
				return nil, fmt.Errorf("decode object ID: %w", err)
			}
		default:
		}
	}

	return ev, nil
}
