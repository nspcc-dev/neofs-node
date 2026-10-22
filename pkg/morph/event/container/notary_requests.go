package container

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// CreateContainerV2Request wraps container creation request to provide
// app-internal event.
type CreateContainerV2Request struct {
	event.Event
	MainTransaction transaction.Transaction

	Container          containerrpc.ContainerInfo
	InvocationScript   []byte
	VerificationScript []byte
	SessionToken       []byte

	// EACLTable is set if container eACL was set in the same
	// transaction container was created. It is nil otherwise.
	EACLTable *PutContainerEACLRequest

	OriginalRequest []byte
}

// RestoreCreateContainerV2Request restores [CreateContainerV2Request] from the
// notary one.
func RestoreCreateContainerV2Request(contractCalls []event.NotaryEvent) (event.Event, error) {
	var (
		res              CreateContainerV2Request
		args             []scparser.PushedItem
		withOptionalEacl bool
		err              error
	)
	switch l := len(contractCalls); l {
	case 1:
	case 2:
		withOptionalEacl = true
	default:
		return nil, fmt.Errorf("unexpected number of contract calls: %d", l)
	}
	cnrCall := contractCalls[0]

	const (
		argNumNoOrigReq   = 4
		argNumWithOrigReq = 2
	)

	switch argNum := len(cnrCall.Params()); argNum {
	case argNumNoOrigReq:
		args, err = event.GetArgs(cnrCall, argNumNoOrigReq)
		if err != nil {
			return nil, err
		}

		if res.InvocationScript, err = event.GetValueFromArg(args, 1, cnrCall.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
		if res.VerificationScript, err = event.GetValueFromArg(args, 2, cnrCall.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
		if res.SessionToken, err = event.GetValueFromArg(args, 3, cnrCall.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
	case argNumWithOrigReq:
		args, err = event.GetArgs(cnrCall, argNumWithOrigReq)
		if err != nil {
			return nil, err
		}

		if res.OriginalRequest, err = event.GetValueFromArg(args, 1, cnrCall.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("wrong parameter count for %s call: expected %d or %d, has %d", cnrCall.Type(), argNumNoOrigReq, argNumWithOrigReq, argNum)
	}

	res.MainTransaction = *cnrCall.Raw().MainTransaction
	if res.Container, err = containerInfoFromPushedItem(args[0]); err != nil {
		return nil, event.WrapInvalidArgError(0, "Container", err)
	}

	if withOptionalEacl {
		ev, err := RestorePutContainerEACLRequest(contractCalls[1])
		if err != nil {
			return nil, fmt.Errorf("additional eACL setting parsing: %w", err)
		}

		evParsed := ev.(PutContainerEACLRequest)
		res.EACLTable = &evParsed
	}

	return res, nil
}

func containerInfoFromPushedItem(instr scparser.PushedItem) (containerrpc.ContainerInfo, error) {
	var (
		res containerrpc.ContainerInfo
		err error
	)

	fields := instr.List
	if len(fields) != 6 {
		return res, fmt.Errorf("wrong number of structure elements: expected 6, got %d", len(fields))
	}

	if res.Version, err = containerAPIVersionFromPushedItem(fields[0]); err != nil {
		return res, event.WrapInvalidArgError(0, "Version", err)
	}
	if res.Owner, err = event.GetValueFromArg(fields, 1, "Owner", scparser.GetUint160FromInstr); err != nil {
		return res, err
	}
	if res.Nonce, err = event.GetValueFromArg(fields, 2, "Nonce", scparser.GetBytesFromInstr); err != nil {
		return res, err
	}
	if res.BasicACL, err = event.GetValueFromArg(fields, 3, "BasicACL", scparser.GetBigIntFromInstr); err != nil {
		return res, err
	}

	attrs := fields[4].List
	if attrs == nil {
		return res, event.WrapInvalidArgError(4, "Attributes", errors.New("not a list"))
	}
	res.Attributes = make([]*containerrpc.ContainerAttribute, len(attrs))
	for i, e := range attrs {
		res.Attributes[i], err = containerAttributeFromPushedItem(e)
		if err != nil {
			return res, event.WrapInvalidArgError(4, fmt.Sprintf("Attributes (#%d)", i), err)
		}
	}

	if res.StoragePolicy, err = event.GetValueFromArg(fields, 5, "StoragePolicy", scparser.GetBytesFromInstr); err != nil {
		return res, err
	}

	return res, nil
}

func containerAPIVersionFromPushedItem(instr scparser.PushedItem) (*containerrpc.ContainerAPIVersion, error) {
	if instr.IsNull() {
		return nil, nil
	}

	fields := instr.List
	if len(fields) != 2 {
		return nil, fmt.Errorf("wrong number of structure elements: expected 2, got %d", len(fields))
	}

	var (
		res = new(containerrpc.ContainerAPIVersion)
		err error
	)
	if res.Major, err = event.GetValueFromArg(fields, 0, "Major", scparser.GetBigIntFromInstr); err != nil {
		return nil, err
	}
	if res.Minor, err = event.GetValueFromArg(fields, 1, "Minor", scparser.GetBigIntFromInstr); err != nil {
		return nil, err
	}
	return res, nil
}

func containerAttributeFromPushedItem(instr scparser.PushedItem) (*containerrpc.ContainerAttribute, error) {
	if instr.IsNull() {
		return nil, nil
	}

	fields := instr.List
	if len(fields) != 2 {
		return nil, fmt.Errorf("wrong number of structure elements: expected 2, got %d", len(fields))
	}

	var (
		res = new(containerrpc.ContainerAttribute)
		err error
	)
	if res.Key, err = event.GetValueFromArg(fields, 0, "Key", scparser.GetUTF8StringFromInstr); err != nil {
		return nil, err
	}
	if res.Value, err = event.GetValueFromArg(fields, 1, "Value", scparser.GetUTF8StringFromInstr); err != nil {
		return nil, err
	}
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
	const (
		argNumNoOrigReq   = 4
		argNumWithOrigReq = 2
	)
	var (
		res  RemoveContainerRequest
		args []scparser.PushedItem
		err  error
	)
	switch argNum := len(notaryReq.Params()); argNum {
	case argNumNoOrigReq:
		args, err = event.GetArgs(notaryReq, argNumNoOrigReq)
		if err != nil {
			return nil, err
		}

		if res.InvocationScript, err = event.GetValueFromArg(args, 1, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
		if res.VerificationScript, err = event.GetValueFromArg(args, 2, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
		if res.SessionToken, err = event.GetValueFromArg(args, 3, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
	case argNumWithOrigReq:
		args, err = event.GetArgs(notaryReq, argNumWithOrigReq)
		if err != nil {
			return nil, err
		}

		if res.OriginalRequest, err = event.GetValueFromArg(args, 1, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("wrong parameter count for %s call: expected %d or %d, has %d", notaryReq.Type(), argNumNoOrigReq, argNumWithOrigReq, argNum)
	}

	res.MainTransaction = *notaryReq.Raw().MainTransaction
	if res.ID, err = event.GetValueFromArg(args, 0, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}

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
	const (
		argNumNoOrigReq   = 4
		argNumWithOrigReq = 2
	)

	var (
		res  PutContainerEACLRequest
		args []scparser.PushedItem
		err  error
	)
	switch argNum := len(notaryReq.Params()); argNum {
	case argNumNoOrigReq:
		args, err = event.GetArgs(notaryReq, argNumNoOrigReq)
		if err != nil {
			return nil, err
		}

		if res.InvocationScript, err = event.GetValueFromArg(args, 1, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
		if res.VerificationScript, err = event.GetValueFromArg(args, 2, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
		if res.SessionToken, err = event.GetValueFromArg(args, 3, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
	case argNumWithOrigReq:
		args, err = event.GetArgs(notaryReq, argNumWithOrigReq)
		if err != nil {
			return nil, err
		}

		if res.OriginalRequest, err = event.GetValueFromArg(args, 1, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("wrong parameter count for %s call: expected %d or %d, has %d", notaryReq.Type(), argNumNoOrigReq, argNumWithOrigReq, argNum)
	}

	res.MainTransaction = *notaryReq.Raw().MainTransaction
	if res.EACL, err = event.GetValueFromArg(args, 0, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}

	return res, nil
}

// SetAttributeRequest wraps attribute setting request to provide app-internal
// event.
type SetAttributeRequest struct {
	event.Event
	MainTransaction transaction.Transaction

	ID                 []byte
	Attribute          string
	Value              string
	ValidUntil         int64
	InvocationScript   []byte
	VerificationScript []byte
	SessionToken       []byte
}

// RestoreSetAttributeRequest restores [SetAttributeRequest] from the notary
// one.
func RestoreSetAttributeRequest(notaryReq event.NotaryEvent) (event.Event, error) {
	const argNum = 7
	args, err := event.GetArgs(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	var res SetAttributeRequest

	if res.ID, err = event.GetValueFromArg(args, 0, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.Attribute, err = event.GetValueFromArg(args, 1, notaryReq.Type().String(), scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.Value, err = event.GetValueFromArg(args, 2, notaryReq.Type().String(), scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.ValidUntil, err = event.GetValueFromArg(args, 3, notaryReq.Type().String(), scparser.GetInt64FromInstr); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = event.GetValueFromArg(args, 4, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = event.GetValueFromArg(args, 5, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.SessionToken, err = event.GetValueFromArg(args, 6, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}

	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
}

// RemoveAttributeRequest wraps attribute removal request to provide
// app-internal event.
type RemoveAttributeRequest struct {
	event.Event
	MainTransaction transaction.Transaction

	ID                 []byte
	Attribute          string
	ValidUntil         int64
	InvocationScript   []byte
	VerificationScript []byte
	SessionToken       []byte
}

// RestoreRemoveAttributeRequest restores [RemoveAttributeRequest] from the
// notary one.
func RestoreRemoveAttributeRequest(notaryReq event.NotaryEvent) (event.Event, error) {
	const argNum = 6
	args, err := event.GetArgs(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	var res RemoveAttributeRequest

	if res.ID, err = event.GetValueFromArg(args, 0, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.Attribute, err = event.GetValueFromArg(args, 1, notaryReq.Type().String(), scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.ValidUntil, err = event.GetValueFromArg(args, 2, notaryReq.Type().String(), scparser.GetInt64FromInstr); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = event.GetValueFromArg(args, 3, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = event.GetValueFromArg(args, 4, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.SessionToken, err = event.GetValueFromArg(args, 5, notaryReq.Type().String(), scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}

	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
}
