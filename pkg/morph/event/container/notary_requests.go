package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func getArgsFromEvent(ne event.NotaryEvent, expectedNum int) ([]event.Op, error) {
	args := ne.Params()
	if len(args) != expectedNum {
		return nil, newWrongArgNumError(expectedNum, len(args))
	}
	return args, nil
}

func getValueFromArg[T any](args []event.Op, i int, desc string, typ stackitem.Type, f func(event.Op) (T, error)) (v T, err error) {
	v, err = f(args[i])
	if err != nil {
		return v, wrapInvalidArgError(i, typ, desc, err)
	}
	return v, nil
}

func wrapInvalidArgError(i int, typ stackitem.Type, desc string, err error) error {
	return fmt.Errorf("arg#%d (%s, %s): %w", i, typ, desc, err)
}

func newWrongArgNumError(expected, actual int) error {
	return fmt.Errorf("wrong/unsupported arg num %d instead of %d", actual, expected)
}

// CreateContainerRequest wraps container creation request to provide
// app-internal event.
type CreateContainerV2Request struct {
	event.Event
	MainTransaction transaction.Transaction

	Container          containerrpc.ContainerInfo
	InvocationScript   []byte
	VerificationScript []byte
	SessionToken       []byte
}

// RestoreCreateContainerV2Request restores [CreateContainerV2Request] from the
// notary one.
func RestoreCreateContainerV2Request(notaryReq event.NotaryEvent) (event.Event, error) {
	testVM := vm.New()
	testVM.LoadScript(notaryReq.ArgumentScript())

	if err := testVM.Run(); err != nil {
		return nil, fmt.Errorf("exec script on test VM: %w", err)
	}

	stack := testVM.Estack()
	const argNum = 4
	if got := stack.Len(); got != argNum {
		return nil, newWrongArgNumError(argNum, got)
	}

	var res CreateContainerV2Request
	var err error

	if err = res.Container.FromStackItem(stack.Pop().Item()); err != nil {
		return nil, wrapInvalidArgError(argNum-1, stackitem.StructT, "container", err)
	}
	if res.InvocationScript, err = stack.Pop().Item().TryBytes(); err != nil {
		return nil, wrapInvalidArgError(argNum-2, stackitem.ByteArrayT, "invocation script", err)
	}
	if res.VerificationScript, err = stack.Pop().Item().TryBytes(); err != nil {
		return nil, wrapInvalidArgError(argNum-3, stackitem.ByteArrayT, "verification script", err)
	}
	if res.SessionToken, err = stack.Pop().Item().TryBytes(); err != nil {
		return nil, wrapInvalidArgError(argNum-4, stackitem.ByteArrayT, "session token", err)
	}
	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
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
