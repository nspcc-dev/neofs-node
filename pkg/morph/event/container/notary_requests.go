package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func getArgsFromEvent(ne event.NotaryEvent, expectedNum int) ([]event.Op, error) {
	args := ne.Params()
	if len(args) != expectedNum {
		return nil, fmt.Errorf("wrong/unsupported arg num %d instead of %d", len(args), expectedNum)
	}
	return args, nil
}

func getValueFromArg[T any](args []event.Op, i int, desc string, typ stackitem.Type, f func(event.Op) (T, error)) (v T, err error) {
	v, err = f(args[i])
	if err != nil {
		return v, fmt.Errorf("arg#%d (%s, %s): %w", i, typ, desc, err)
	}
	return v, nil
}

// CreateContainerRequest wraps container creation request to provide
// app-internal event.
type CreateContainerRequest struct {
	event.Event
	MainTransaction transaction.Transaction
	fschaincontracts.CreateContainerParams
}

// RestoreCreateContainerRequest restores [CreateContainerRequest] from the
// notary one.
func RestoreCreateContainerRequest(notaryReq event.NotaryEvent) (event.Event, error) {
	const argNum = 7
	args, err := getArgsFromEvent(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	var res CreateContainerRequest

	if res.Container, err = getValueFromArg(args, argNum-1, "container", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, argNum-2, "invocation script", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, argNum-3, "verification script", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, argNum-4, "session token", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.DomainName, err = getValueFromArg(args, argNum-5, "domain name", stackitem.ByteArrayT, event.StringFromOpcode); err != nil {
		return nil, err
	}
	if res.DomainZone, err = getValueFromArg(args, argNum-6, "domain zone", stackitem.ByteArrayT, event.StringFromOpcode); err != nil {
		return nil, err
	}
	if res.EnableObjectMetadata, err = getValueFromArg(args, argNum-7, "enable object metadata", stackitem.BooleanT, event.BoolFromOpcode); err != nil {
		return nil, err
	}
	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
}

// RemoveContainerRequest wraps container removal request to provide
// app-internal event.
type RemoveContainerRequest struct {
	event.Event
	MainTransaction transaction.Transaction
	fschaincontracts.RemoveContainerParams
}

// RestoreRemoveContainerRequest restores [RemoveContainerRequest] from the
// notary one.
func RestoreRemoveContainerRequest(notaryReq event.NotaryEvent) (event.Event, error) {
	const argNum = 4
	args, err := getArgsFromEvent(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	var res RemoveContainerRequest

	if res.ID, err = getValueFromArg(args, argNum-1, "container ID", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, argNum-2, "invocation script", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, argNum-3, "verification script", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, argNum-4, "session token", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
}

// PutContainerEACLRequest wraps container EACL setting request to provide
// app-internal event.
type PutContainerEACLRequest struct {
	event.Event
	MainTransaction transaction.Transaction
	fschaincontracts.PutContainerEACLParams
}

// RestorePutContainerEACLRequest restores [PutContainerEACLRequest] from the
// notary one.
func RestorePutContainerEACLRequest(notaryReq event.NotaryEvent) (event.Event, error) {
	const argNum = 4
	args, err := getArgsFromEvent(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	var res PutContainerEACLRequest

	if res.EACL, err = getValueFromArg(args, argNum-1, "eACL", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, argNum-2, "invocation script", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, argNum-3, "verification script", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, argNum-4, "session token", stackitem.ByteArrayT, event.BytesFromOpcode); err != nil {
		return nil, err
	}
	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
}
