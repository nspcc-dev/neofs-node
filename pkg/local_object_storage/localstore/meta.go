package localstore

import (
	"encoding/binary"
	"io"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
)

// ObjectMeta represents meta information about
// the object that is stored in meta storage.
type ObjectMeta struct {
	head *object.Object

	savedAtEpoch uint64
}

// SavedAtEpoch returns the number of epoch
// at which the object was saved locally.
func (m *ObjectMeta) SavedAtEpoch() uint64 {
	if m != nil {
		return m.savedAtEpoch
	}

	return 0
}

// Head returns the object w/o payload.
func (m *ObjectMeta) Head() *object.Object {
	if m != nil {
		return m.head
	}

	return nil
}

// AddressFromMeta extracts the Address from object meta.
func AddressFromMeta(m *ObjectMeta) *objectSDK.Address {
	return m.Head().Address()
}

func metaFromObject(o *object.Object) *ObjectMeta {
	// FIXME: remove hard-code
	meta := new(ObjectMeta)
	meta.savedAtEpoch = 10

	raw := object.NewRaw()
	raw.SetID(o.GetID())
	raw.SetContainerID(o.GetContainerID())
	raw.SetOwnerID(o.GetOwnerID())
	// TODO: set other meta fields

	meta.head = raw.Object()

	return meta
}

func metaToBytes(m *ObjectMeta) ([]byte, error) {
	data := make([]byte, 8)

	binary.BigEndian.PutUint64(data, m.savedAtEpoch)

	objBytes, err := objectBytes(m.head)
	if err != nil {
		return nil, err
	}

	return append(data, objBytes...), nil
}

func metaFromBytes(data []byte) (*ObjectMeta, error) {
	if len(data) < 8 {
		return nil, io.ErrUnexpectedEOF
	}

	obj, err := object.FromBytes(data[8:])
	if err != nil {
		return nil, errors.Wrap(err, "could not get object address from bytes")
	}

	meta := new(ObjectMeta)
	meta.savedAtEpoch = binary.BigEndian.Uint64(data)
	meta.head = obj

	return meta, nil
}
