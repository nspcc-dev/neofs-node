package putsvc

import (
	"bytes"
	"crypto/sha256"
	"hash"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/pkg/errors"
)

type validatingTarget struct {
	nextTarget transformer.ObjectTarget

	fmt *object.FormatValidator

	hash hash.Hash

	checksum []byte
}

func (t *validatingTarget) WriteHeader(obj *object.RawObject) error {
	cs := obj.PayloadChecksum()
	switch typ := cs.Type(); typ {
	default:
		return errors.Errorf("(%T) unsupported payload checksum type %v", t, typ)
	case pkg.ChecksumSHA256:
		t.hash = sha256.New()
	case pkg.ChecksumTZ:
		t.hash = tz.New()
	}

	t.checksum = cs.Sum()

	if err := t.fmt.Validate(obj.Object()); err != nil {
		return errors.Wrapf(err, "(%T) coult not validate object format", t)
	}

	return t.nextTarget.WriteHeader(obj)
}

func (t *validatingTarget) Write(p []byte) (n int, err error) {
	n, err = t.hash.Write(p)
	if err != nil {
		return
	}

	return t.nextTarget.Write(p)
}

func (t *validatingTarget) Close() (*transformer.AccessIdentifiers, error) {
	if !bytes.Equal(t.hash.Sum(nil), t.checksum) {
		return nil, errors.Errorf("(%T) incorrect payload checksum", t)
	}

	return t.nextTarget.Close()
}
