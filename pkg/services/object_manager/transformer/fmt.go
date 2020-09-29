package transformer

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
)

type formatter struct {
	nextTarget ObjectTarget

	key *ecdsa.PrivateKey

	obj *object.RawObject

	sz uint64
}

// NewFormatTarget returns ObjectTarget instance that finalizes object structure
// and writes it to the next target.
//
// Chunks must be written before the WriteHeader call.
//
// Object changes:
// - sets version to current SDK version;
// - sets payload size to the total length of all written chunks;
// - calculates and sets verification fields (ID, Signature).
func NewFormatTarget(key *ecdsa.PrivateKey, nextTarget ObjectTarget) ObjectTarget {
	return &formatter{
		nextTarget: nextTarget,
		key:        key,
	}
}

func (f *formatter) WriteHeader(obj *object.RawObject) error {
	f.obj = obj

	return nil
}

func (f *formatter) Write(p []byte) (n int, err error) {
	n, err = f.nextTarget.Write(p)

	f.sz += uint64(n)

	return
}

func (f *formatter) Close() (*AccessIdentifiers, error) {
	f.obj.SetVersion(pkg.SDKVersion())
	f.obj.SetPayloadSize(f.sz)

	var parID *objectSDK.ID

	if par := f.obj.GetParent(); par != nil && par.ToV2().GetHeader() != nil {
		rawPar := objectSDK.NewRawFromV2(par.ToV2())

		if err := objectSDK.SetIDWithSignature(f.key, rawPar); err != nil {
			return nil, errors.Wrap(err, "could not finalize parent object")
		}

		parID = rawPar.GetID()

		f.obj.SetParent(rawPar.Object())
	}

	if err := objectSDK.SetIDWithSignature(f.key, f.obj.SDK()); err != nil {
		return nil, errors.Wrap(err, "could not finalize object")
	}

	if err := f.nextTarget.WriteHeader(f.obj); err != nil {
		return nil, errors.Wrap(err, "could not write header to next target")
	}

	if _, err := f.nextTarget.Close(); err != nil {
		return nil, errors.Wrap(err, "could not close next target")
	}

	return new(AccessIdentifiers).
		WithSelfID(f.obj.GetID()).
		WithParentID(parID), nil
}
