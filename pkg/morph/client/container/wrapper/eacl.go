package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	v2ACL "github.com/nspcc-dev/neofs-api-go/v2/acl"
	msgACL "github.com/nspcc-dev/neofs-api-go/v2/acl/grpc"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/pkg/errors"
)

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

	grpcMsg := new(msgACL.EACLTable)
	err = grpcMsg.Unmarshal(rpcAnswer.EACL())
	if err != nil {
		// use other major version if there any
		return nil, nil, err
	}

	v2table := v2ACL.TableFromGRPCMessage(grpcMsg)

	return eacl.NewTableFromV2(v2table), rpcAnswer.Signature(), nil
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

	if v2 := table.ToV2(); v2 == nil {
		return errUnsupported // use other major version if there any
	} else {
		data, err := v2.StableMarshal(nil)
		if err != nil {
			return errors.Wrap(err, "can't marshal eacl table")
		}

		args.SetEACL(data)
	}

	return w.client.SetEACL(args)
}
