package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	containerrpc "github.com/nspcc-dev/neofs-contract/rpc/container"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

func getArgsFromEvent(ne event.NotaryEvent, expectedNum int) ([]scparser.PushedItem, error) {
	args := ne.Params()
	if len(args) != expectedNum {
		return nil, event.WrongNumberOfParameters(expectedNum, len(args))
	}
	return args, nil
}

func getValueFromArg[T any](args []scparser.PushedItem, i int, desc string, f scparser.GetEFromInstr[T]) (v T, err error) {
	v, err = f(args[i].Instruction)
	if err != nil {
		return v, wrapInvalidArgError(i, desc, err)
	}
	return v, nil
}

func wrapInvalidArgError(i int, desc string, err error) error {
	return fmt.Errorf("arg#%d (%s): %w", i, desc, err)
}

// CreateContainerV2Request wraps container creation request to provide
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
	const argNum = 4
	var (
		res CreateContainerV2Request
		err error
	)

	args, err := getArgsFromEvent(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	if res.Container, err = containerInfoFromPushedItem(args[0]); err != nil {
		return nil, wrapInvalidArgError(0, "container", err)
	}
	if res.InvocationScript, err = scparser.GetBytesFromInstr(args[1].Instruction); err != nil {
		return nil, wrapInvalidArgError(1, "invocation script", err)
	}
	if res.VerificationScript, err = scparser.GetBytesFromInstr(args[2].Instruction); err != nil {
		return nil, wrapInvalidArgError(2, "verification script", err)
	}
	if res.SessionToken, err = scparser.GetBytesFromInstr(args[3].Instruction); err != nil {
		return nil, wrapInvalidArgError(3, "session token", err)
	}
	res.MainTransaction = *notaryReq.Raw().MainTransaction

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

	res.Version, err = containerAPIVersionFromPushedItem(fields[0])
	if err != nil {
		return res, fmt.Errorf("field Version: %w", err)
	}
	res.Owner, err = scparser.GetUint160FromInstr(fields[1].Instruction)
	if err != nil {
		return res, fmt.Errorf("field Owner: %w", err)
	}
	res.Nonce, err = scparser.GetBytesFromInstr(fields[2].Instruction)
	if err != nil {
		return res, fmt.Errorf("field Nonce: %w", err)
	}
	res.BasicACL, err = scparser.GetBigIntFromInstr(fields[3].Instruction)
	if err != nil {
		return res, fmt.Errorf("field BasicACL: %w", err)
	}

	attrs := fields[4].List
	if attrs == nil {
		return res, fmt.Errorf("field Attributes: not a list")
	}
	res.Attributes = make([]*containerrpc.ContainerAttribute, len(attrs))
	for i, e := range attrs {
		res.Attributes[i], err = containerAttributeFromPushedItem(e)
		if err != nil {
			return res, fmt.Errorf("field Attributes (#%d): %w", i, err)
		}
	}

	res.StoragePolicy, err = scparser.GetBytesFromInstr(fields[5].Instruction)
	if err != nil {
		return res, fmt.Errorf("field StoragePolicy: %w", err)
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

	maj, err := scparser.GetBigIntFromInstr(fields[0].Instruction)
	if err != nil {
		return nil, fmt.Errorf("field Major: %w", err)
	}
	minor, err := scparser.GetBigIntFromInstr(fields[1].Instruction)
	if err != nil {
		return nil, fmt.Errorf("field Minor: %w", err)
	}
	return &containerrpc.ContainerAPIVersion{
		Major: maj,
		Minor: minor,
	}, nil
}

func containerAttributeFromPushedItem(instr scparser.PushedItem) (*containerrpc.ContainerAttribute, error) {
	if instr.IsNull() {
		return nil, nil
	}

	fields := instr.List
	if len(fields) != 2 {
		return nil, fmt.Errorf("wrong number of structure elements: expected 2, got %d", len(fields))
	}
	k, err := scparser.GetUTF8StringFromInstr(fields[0].Instruction)
	if err != nil {
		return nil, fmt.Errorf("attribute key: %w", err)
	}
	v, err := scparser.GetUTF8StringFromInstr(fields[1].Instruction)
	if err != nil {
		return nil, fmt.Errorf("attribute value: %w", err)
	}

	return &containerrpc.ContainerAttribute{
		Key:   k,
		Value: v,
	}, nil
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

	if res.Container, err = getValueFromArg(args, 0, "container", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, 1, "invocation script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, 2, "verification script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, 3, "session token", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.DomainName, err = getValueFromArg(args, 4, "domain name", scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.DomainZone, err = getValueFromArg(args, 5, "domain zone", scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.EnableObjectMetadata, err = getValueFromArg(args, 6, "enable object metadata", scparser.GetBoolFromInstr); err != nil {
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

	if res.ID, err = getValueFromArg(args, 0, "container ID", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, 1, "invocation script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, 2, "verification script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, 3, "session token", scparser.GetBytesFromInstr); err != nil {
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

	if res.EACL, err = getValueFromArg(args, 0, "eACL", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, 1, "invocation script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, 2, "verification script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, 3, "session token", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
}

// AddStructsRequest wraps container protobuf->struct migration request to
// provide app-internal event.
type AddStructsRequest struct {
	event.Event
	MainTransaction transaction.Transaction
}

// RestoreAddStructsRequest restores [AddStructsRequest] from the
// notary one.
func RestoreAddStructsRequest(notaryReq event.NotaryEvent) (event.Event, error) {
	_, err := getArgsFromEvent(notaryReq, 0)
	if err != nil {
		return nil, err
	}

	return AddStructsRequest{
		MainTransaction: *notaryReq.Raw().MainTransaction,
	}, nil
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
	args, err := getArgsFromEvent(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	var res SetAttributeRequest

	if res.ID, err = getValueFromArg(args, 0, "ID", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.Attribute, err = getValueFromArg(args, 1, "attribute", scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.Value, err = getValueFromArg(args, 2, "value", scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.ValidUntil, err = getValueFromArg(args, 3, "request expiration time", scparser.GetInt64FromInstr); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, 4, "invocation script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, 5, "verification script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, 6, "session token", scparser.GetBytesFromInstr); err != nil {
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
	args, err := getArgsFromEvent(notaryReq, argNum)
	if err != nil {
		return nil, err
	}

	var res RemoveAttributeRequest

	if res.ID, err = getValueFromArg(args, 0, "ID", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.Attribute, err = getValueFromArg(args, 1, "attribute", scparser.GetStringFromInstr); err != nil {
		return nil, err
	}
	if res.ValidUntil, err = getValueFromArg(args, 2, "request expiration time", scparser.GetInt64FromInstr); err != nil {
		return nil, err
	}
	if res.InvocationScript, err = getValueFromArg(args, 3, "invocation script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.VerificationScript, err = getValueFromArg(args, 4, "verification script", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}
	if res.SessionToken, err = getValueFromArg(args, 5, "session token", scparser.GetBytesFromInstr); err != nil {
		return nil, err
	}

	res.MainTransaction = *notaryReq.Raw().MainTransaction

	return res, nil
}
