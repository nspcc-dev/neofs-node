package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	staticli "github.com/nspcc-dev/neofs-node/pkg/morph/client"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/signature"
)

// GetEACL reads the extended ACL table from NeoFS system
// through Container contract call.
func (w *Wrapper) GetEACL(cid *cid.ID) (*eacl.Table, error) {
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

	table := eacl.NewTable()
	if err = table.Unmarshal(rpcAnswer.EACL()); err != nil {
		// use other major version if there any
		return nil, err
	}

	binToken := rpcAnswer.SessionToken()
	if len(binToken) > 0 {
		tok := session.NewToken()

		err = tok.Unmarshal(binToken)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal session token: %w", err)
		}

		table.SetSessionToken(tok)
	}

	tableSignature := signature.New()
	tableSignature.SetKey(rpcAnswer.PublicKey())
	tableSignature.SetSign(sig)

	table.SetSignature(tableSignature)

	return table, nil
}

// PutEACL marshals table, and passes it to Wrapper's PutEACLBinary method
// along with sig.Key() and sig.Sign().
//
// Returns error if table is nil.
//
// If TryNotary is provided, calls notary contract.
func PutEACL(w *Wrapper, table *eacl.Table) error {
	if table == nil {
		return errNilArgument
	}

	data, err := table.Marshal()
	if err != nil {
		return fmt.Errorf("can't marshal eacl table: %w", err)
	}

	binToken, err := table.SessionToken().Marshal()
	if err != nil {
		return fmt.Errorf("could not marshal session token: %w", err)
	}

	sig := table.Signature()

	return w.PutEACL(
		PutEACLPrm{
			table: data,
			key:   sig.Key(),
			sig:   sig.Sign(),
			token: binToken,
		})
}

// PutEACLPrm groups parameters of PutEACL operation.
type PutEACLPrm struct {
	table []byte
	key   []byte
	sig   []byte
	token []byte

	staticli.InvokePrmOptional
}

// SetTable sets table.
func (p *PutEACLPrm) SetTable(table []byte) {
	p.table = table
}

// SetKey sets key.
func (p *PutEACLPrm) SetKey(key []byte) {
	p.key = key
}

// SetSignature sets signature.
func (p *PutEACLPrm) SetSignature(sig []byte) {
	p.sig = sig
}

// SetToken sets session token.
func (p *PutEACLPrm) SetToken(token []byte) {
	p.token = token
}

// PutEACL saves binary eACL table with its session token, key and signature
// in NeoFS system through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (w *Wrapper) PutEACL(prm PutEACLPrm) error {
	if len(prm.sig) == 0 || len(prm.key) == 0 {
		return errNilArgument
	}

	args := client.SetEACLArgs{}
	args.SetSignature(prm.sig)
	args.SetPublicKey(prm.key)
	args.SetEACL(prm.table)
	args.SetSessionToken(prm.token)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.SetEACL(args)
}
