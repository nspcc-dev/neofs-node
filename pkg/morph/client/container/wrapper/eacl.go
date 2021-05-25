package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	containerSDK "github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
)

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
func (w *Wrapper) GetEACL(cid *containerSDK.ID) (*eacl.Table, error) {
	if cid == nil {
		return nil, errNilArgument
	}

	args := client.EACLArgs{}

	v2 := cid.ToV2()
	if v2 == nil {
		return nil, errUnsupported // use other major version if there any
	}

	args.SetCID(v2.GetValue())

	rpcAnswer, err := w.client.EACL(args)
	if err != nil {
		return nil, err
	}

	// Client may not return errors if the table is missing, so check this case additionally.
	// The absence of a signature in the response can be taken as an eACL absence criterion,
	// since unsigned table cannot be approved in the storage by design.
	sig := rpcAnswer.Signature()
	if len(sig) == 0 {
		return nil, container.ErrEACLNotFound
	}

	tableSignature := pkg.NewSignature()
	tableSignature.SetKey(rpcAnswer.PublicKey())
	tableSignature.SetSign(sig)

	table := eacl.NewTable()
	if err = table.Unmarshal(rpcAnswer.EACL()); err != nil {
		// use other major version if there any
		return nil, err
	}

	table.SetSignature(tableSignature)

	return table, nil
}

// PutEACL marshals table, and passes it to Wrapper's PutEACLBinary method
// along with sig.Key() and sig.Sign().
//
// Returns error if table is nil.
//
// If TryNotary is provided, calls notary contract.
func PutEACL(w *Wrapper, table *eacl.Table, sig *pkg.Signature) error {
	if table == nil {
		return errNilArgument
	}

	data, err := table.Marshal()
	if err != nil {
		return fmt.Errorf("can't marshal eacl table: %w", err)
	}

	return w.PutEACL(data, sig.Key(), sig.Sign())
}

// PutEACL saves binary eACL table with its key and signature
// in NeoFS system through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (w *Wrapper) PutEACL(table, key, sig []byte) error {
	if len(sig) == 0 || len(key) == 0 {
		return errNilArgument
	}

	args := client.SetEACLArgs{}
	args.SetSignature(sig)
	args.SetPublicKey(key)
	args.SetEACL(table)

	return w.client.SetEACL(args)
}
