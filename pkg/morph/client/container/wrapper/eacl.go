package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/pkg/errors"
)

var ErrEACLNotFound = errors.New("extended ACL table is not set for this container")

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
func (w *Wrapper) GetEACL(cid *container.ID) (*eacl.Table, []byte, error) {
	if cid == nil {
		return nil, nil, errNilArgument
	}

	args := client.EACLArgs{}

	if v2 := cid.ToV2(); v2 == nil {
		return nil, nil, errUnsupported // use other major version if there any
	} else {
		args.SetCID(v2.GetValue())
	}

	rpcAnswer, err := w.client.EACL(args)
	if err != nil {
		return nil, nil, err
	}

	// Client may not return errors if the table is missing, so check this case additionally.
	// The absence of a signature in the response can be taken as an eACL absence criterion,
	// since unsigned table cannot be approved in the storage by design.
	sig := rpcAnswer.Signature()
	if len(sig) == 0 {
		return nil, nil, ErrEACLNotFound
	}

	table := eacl.NewTable()
	if err = table.Unmarshal(rpcAnswer.EACL()); err != nil {
		// use other major version if there any
		return nil, nil, err
	}

	return table, sig, nil
}

// PutEACL saves the extended ACL table in NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (w *Wrapper) PutEACL(table *eacl.Table, signature []byte) error {
	if table == nil || len(signature) == 0 {
		return errNilArgument
	}

	args := client.SetEACLArgs{}
	args.SetSignature(signature)

	data, err := table.Marshal()
	if err != nil {
		return errors.Wrap(err, "can't marshal eacl table")
	}

	args.SetEACL(data)

	return w.client.SetEACL(args)
}
