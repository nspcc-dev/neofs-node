package subnetevents

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Put structures information about the notification generated by Put method of Subnet contract.
type Put struct {
	notaryRequest *payload.P2PNotaryRequest

	txHash util.Uint256

	id []byte

	owner []byte

	info []byte
}

// MorphEvent implements Neo:Morph Event interface.
func (Put) MorphEvent() {}

// ID returns identifier of the creating subnet in a binary format of NeoFS API protocol.
func (x Put) ID() []byte {
	return x.id
}

// Owner returns subnet owner's public key in a binary format.
func (x Put) Owner() []byte {
	return x.owner
}

// Info returns information about the subnet in a binary format of NeoFS API protocol.
func (x Put) Info() []byte {
	return x.info
}

// TxHash returns hash of the transaction which thrown the notification event.
// Makes sense only in notary environments.
func (x Put) TxHash() util.Uint256 {
	return x.txHash
}

// NotaryMainTx returns main transaction of the request in the Notary service.
// Returns nil in non-notary environments.
func (x Put) NotaryMainTx() *transaction.Transaction {
	if x.notaryRequest != nil {
		return x.notaryRequest.MainTransaction
	}

	return nil
}

// number of items in notification about subnet creation.
const itemNumPut = 3

// ParsePut parses the notification about the creation of a subnet which has been thrown
// by the appropriate method of the subnet contract.
//
// Resulting event is of Put type.
func ParsePut(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		put Put
		err error
	)

	items, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("parse stack array: %w", err)
	}

	if ln := len(items); ln != itemNumPut {
		return nil, event.WrongNumberOfParameters(itemNumPut, ln)
	}

	// parse ID
	put.id, err = client.BytesFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("id item: %w", err)
	}

	// parse owner
	put.owner, err = client.BytesFromStackItem(items[1])
	if err != nil {
		return nil, fmt.Errorf("owner item: %w", err)
	}

	// parse info about subnet
	put.info, err = client.BytesFromStackItem(items[2])
	if err != nil {
		return nil, fmt.Errorf("info item: %w", err)
	}

	put.txHash = e.Container

	return put, nil
}

// ParseNotaryPut parses the notary notification about the creation of a subnet which has been
// thrown by the appropriate method of the subnet contract.
//
// Resulting event is of Put type.
func ParseNotaryPut(e event.NotaryEvent) (event.Event, error) {
	var put Put

	put.notaryRequest = e.Raw()
	if put.notaryRequest == nil {
		panic(fmt.Sprintf("nil %T in notary environment", put.notaryRequest))
	}

	var (
		err error

		prms = e.Params()
	)

	if ln := len(prms); ln != itemNumPut {
		return nil, event.WrongNumberOfParameters(itemNumPut, ln)
	}

	// parse info about subnet
	put.info, err = event.BytesFromOpcode(prms[0])
	if err != nil {
		return nil, fmt.Errorf("info param: %w", err)
	}

	// parse owner
	put.owner, err = event.BytesFromOpcode(prms[1])
	if err != nil {
		return nil, fmt.Errorf("creator param: %w", err)
	}

	// parse ID
	put.id, err = event.BytesFromOpcode(prms[2])
	if err != nil {
		return nil, fmt.Errorf("id param: %w", err)
	}

	return put, nil
}
