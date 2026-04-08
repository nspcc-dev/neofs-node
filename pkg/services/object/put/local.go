package putsvc

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectStorage is an object storage interface.
type ObjectStorage interface {
	// Put must save passed object
	// and return any appeared error.
	//
	// Optional objBin parameter carries object encoded in a canonical NeoFS binary
	// format.
	Put(obj *object.Object, objBin []byte) error
	// IsLocked must clarify object's lock status.
	IsLocked(oid.Address) (bool, error)
}

func putObjectLocally(storage ObjectStorage, obj *object.Object, enc *encodedObject) error {
	var objBin []byte
	if enc != nil && enc.pldOff > 0 {
		objBin = enc.b[enc.hdrOff:]
	}

	if err := storage.Put(obj, objBin); err != nil {
		return fmt.Errorf("could not put object to local storage: %w", err)
	}

	return nil
}

// ValidateAndStoreObjectLocally checks format of given object and, if it's
// correct, stores it in the underlying local object storage. Serves operation
// similar to local-only [Service.Put] one.
func (p *Service) ValidateAndStoreObjectLocally(obj object.Object) error {
	cnrID := obj.GetContainerID()
	if cnrID.IsZero() {
		return errors.New("missing container ID")
	}

	cs, csSet := obj.PayloadChecksum()
	if !csSet {
		return errors.New("missing payload checksum")
	}

	csType := cs.Type()
	switch csType {
	default:
		return errors.New("unknown payload checksum type")
	case checksum.TillichZemor:
		return errors.New("object has unsupported Tillich-Zémor checksum")
	case
		checksum.SHA256:
	}

	maxPayloadSz := p.maxSizeSrc.MaxObjectSize()
	if maxPayloadSz == 0 {
		return errors.New("failed to obtain max payload size setting")
	}

	payload := obj.Payload()
	payloadSz := obj.PayloadSize()
	if payloadSz != uint64(len(payload)) {
		return ErrWrongPayloadSize
	}

	if payloadSz > maxPayloadSz {
		return ErrExceedingMaxSize
	}

	if err := p.fmtValidator.Validate(&obj, false, true); err != nil {
		return fmt.Errorf("validate object format: %w", err)
	}

	err := p.fmtValidator.ValidateContent(&obj)
	if err != nil {
		return fmt.Errorf("validate payload content: %w", err)
	}

	// checksum must be only SHA256, this was checked above
	h := sha256.Sum256(payload)
	if !bytes.Equal(h[:], cs.Value()) {
		return errors.New("payload SHA-256 checksum mismatch")
	}

	return putObjectLocally(p.localStore, &obj, nil)
}
