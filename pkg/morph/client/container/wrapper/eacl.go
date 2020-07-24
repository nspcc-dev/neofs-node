package wrapper

import (
	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/pkg/errors"
)

// Table represents extended ACL rule table.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage.Table.
type Table = storage.Table

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
func (w *Wrapper) GetEACL(cid CID) (Table, error) {
	// prepare invocation arguments
	args := contract.EACLArgs{}
	args.SetCID(cid.Bytes())

	// invoke smart contract call
	values, err := w.client.EACL(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not invoke smart contract")
	}

	// unmarshal and return eACL table
	return eacl.UnmarshalTable(values.EACL())
}

// PutEACL saves the extended ACL table in NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (w *Wrapper) PutEACL(cid CID, table Table, sig []byte) error {
	// prepare invocation arguments
	args := contract.SetEACLArgs{}
	args.SetEACL(eacl.MarshalTable(table))
	args.SetCID(cid.Bytes())
	args.SetSignature(sig)

	// invoke smart contract call
	//
	// Note: errors.Wrap return nil on nil error arg.
	return errors.Wrap(
		w.client.SetEACL(args),
		"could not invoke smart contract",
	)
}
