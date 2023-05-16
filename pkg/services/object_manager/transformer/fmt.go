package transformer

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

type formatter struct {
	prm *FormatterParams

	obj *object.Object

	sz uint64
}

// FormatterParams groups NewFormatTarget parameters.
type FormatterParams struct {
	Key *ecdsa.PrivateKey

	NextTarget ObjectTarget

	SessionToken *session.Object

	NetworkState netmap.State
}

// NewFormatTarget returns ObjectTarget instance that finalizes object structure
// and writes it to the next target.
//
// Chunks must be written before the WriteHeader call.
//
// Object changes:
// - sets version to current SDK version;
// - sets payload size to the total length of all written chunks;
// - sets session token;
// - sets number of creation epoch;
// - calculates and sets verification fields (ID, Signature).
func NewFormatTarget(p *FormatterParams) ObjectTarget {
	return &formatter{
		prm: p,
	}
}

func (f *formatter) WriteHeader(obj *object.Object) error {
	f.obj = obj

	return nil
}

func (f *formatter) Write(p []byte) (n int, err error) {
	n, err = f.prm.NextTarget.Write(p)

	f.sz += uint64(n)

	return
}

func (f *formatter) Close() (*AccessIdentifiers, error) {
	curEpoch := f.prm.NetworkState.CurrentEpoch()
	ver := version.Current()

	f.obj.SetVersion(&ver)
	f.obj.SetPayloadSize(f.sz)
	f.obj.SetSessionToken(f.prm.SessionToken)
	f.obj.SetCreationEpoch(curEpoch)

	var (
		parID  *oid.ID
		parHdr *object.Object
	)

	signer := neofsecdsa.SignerRFC6979(*f.prm.Key)

	if par := f.obj.Parent(); par != nil && par.Signature() == nil {
		rawPar := object.NewFromV2(par.ToV2())

		rawPar.SetSessionToken(f.prm.SessionToken)
		rawPar.SetCreationEpoch(curEpoch)

		if err := object.SetIDWithSignature(signer, rawPar); err != nil {
			return nil, fmt.Errorf("could not finalize parent object: %w", err)
		}

		id, _ := rawPar.ID()
		parID = &id
		parHdr = rawPar

		f.obj.SetParent(parHdr)
	}

	if err := object.SetIDWithSignature(signer, f.obj); err != nil {
		return nil, fmt.Errorf("could not finalize object: %w", err)
	}

	if err := f.prm.NextTarget.WriteHeader(f.obj); err != nil {
		return nil, fmt.Errorf("could not write header to next target: %w", err)
	}

	if _, err := f.prm.NextTarget.Close(); err != nil {
		return nil, fmt.Errorf("could not close next target: %w", err)
	}

	id, _ := f.obj.ID()

	return new(AccessIdentifiers).
		WithSelfID(id).
		WithParentID(parID).
		WithParent(parHdr), nil
}
