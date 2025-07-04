package putsvc

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/tzhash/tz"
)

// ObjectStorage is an object storage interface.
type ObjectStorage interface {
	// Put must save passed object
	// and return any appeared error.
	//
	// Optional objBin parameter carries object encoded in a canonical NeoFS binary
	// format.
	Put(obj *object.Object, objBin []byte) error
	// Delete must delete passed objects
	// and return any appeared error.
	Delete(tombstone oid.Address, tombExpiration uint64, toDelete []oid.ID) error
	// Lock must lock passed objects
	// and return any appeared error.
	Lock(locker oid.Address, toLock []oid.ID) error
	// IsLocked must clarify object's lock status.
	IsLocked(oid.Address) (bool, error)
}

type localTarget struct {
	storage ObjectStorage
}

func (t *localTarget) WriteObject(obj *object.Object, meta objectCore.ContentMeta, enc encodedObject) ([]byte, error) {
	err := putObjectLocally(t.storage, obj, meta, &enc)
	return nil, err
}

func putObjectLocally(storage ObjectStorage, obj *object.Object, meta objectCore.ContentMeta, enc *encodedObject) error {
	switch obj.Type() {
	case object.TypeTombstone:
		exp, err := objectCore.Expiration(*obj)
		if err != nil && !errors.Is(err, objectCore.ErrNoExpiration) {
			return fmt.Errorf("reading tombstone expiration: %w", err)
		}

		err = storage.Delete(objectCore.AddressOf(obj), exp, meta.Objects())
		if err != nil {
			return fmt.Errorf("could not delete objects from tombstone locally: %w", err)
		}
	case object.TypeLock:
		err := storage.Lock(objectCore.AddressOf(obj), meta.Objects())
		if err != nil {
			return fmt.Errorf("could not lock object from lock objects locally: %w", err)
		}
	default:
		// objects that do not change meta storage
	}

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
		return errors.New("unsupported payload checksum type")
	case
		checksum.SHA256,
		checksum.TillichZemor:
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

	cnr, err := p.cnrSrc.Get(cnrID)
	if err != nil {
		return fmt.Errorf("read container by ID: %w", err)
	}

	if !cnr.IsHomomorphicHashingDisabled() {
		csHomo, csHomoSet := obj.PayloadHomomorphicHash()
		switch {
		case !csHomoSet:
			return errors.New("missing homomorphic payload checksum")
		case csHomo.Type() != checksum.TillichZemor:
			return fmt.Errorf("wrong/unsupported type of homomorphic payload checksum, expected %s", checksum.TillichZemor)
		case len(csHomo.Value()) != tz.Size:
			return fmt.Errorf("invalid/unsupported length of %s homomorphic payload checksum, expected %d",
				csHomo.Type(), tz.Size)
		}
	}

	if err := p.fmtValidator.Validate(&obj, false); err != nil {
		return fmt.Errorf("validate object format: %w", err)
	}

	objMeta, err := p.fmtValidator.ValidateContent(&obj)
	if err != nil {
		return fmt.Errorf("validate payload content: %w", err)
	}

	//nolint:exhaustive
	switch csType {
	case checksum.SHA256:
		h := sha256.Sum256(payload)
		if !bytes.Equal(h[:], cs.Value()) {
			return errors.New("payload SHA-256 checksum mismatch")
		}
	case checksum.TillichZemor:
		h := tz.Sum(payload)
		if !bytes.Equal(h[:], cs.Value()) {
			return errors.New("payload Tillich-Zemor checksum mismatch")
		}
	}

	return putObjectLocally(p.localStore, &obj, objMeta, nil)
}
