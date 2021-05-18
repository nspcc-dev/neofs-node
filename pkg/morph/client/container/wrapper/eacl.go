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
func (w *Wrapper) GetEACL(cid *containerSDK.ID) (*eacl.Table, *pkg.Signature, error) {
	if cid == nil {
		return nil, nil, errNilArgument
	}

	args := client.EACLArgs{}

	v2 := cid.ToV2()
	if v2 == nil {
		return nil, nil, errUnsupported // use other major version if there any
	}

	args.SetCID(v2.GetValue())

	rpcAnswer, err := w.client.EACL(args)
	if err != nil {
		return nil, nil, err
	}

	// Client may not return errors if the table is missing, so check this case additionally.
	// The absence of a signature in the response can be taken as an eACL absence criterion,
	// since unsigned table cannot be approved in the storage by design.
	sig := rpcAnswer.Signature()
	if len(sig) == 0 {
		return nil, nil, container.ErrEACLNotFound
	}

	tableSignature := pkg.NewSignature()
	tableSignature.SetKey(rpcAnswer.PublicKey())
	tableSignature.SetSign(sig)

	table := eacl.NewTable()
	if err = table.Unmarshal(rpcAnswer.EACL()); err != nil {
		// use other major version if there any
		return nil, nil, err
	}

	return table, tableSignature, nil
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
		return fmt.Errorf("can't marshal eacl table: %w", err)
	}

	args.SetEACL(data)

	return w.client.SetEACL(args)
}
