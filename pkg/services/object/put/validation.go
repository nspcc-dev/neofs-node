package putsvc

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/tzhash/tz"
)

type validatingTarget struct {
	nextTarget transformer.ObjectTarget

	fmt *object.FormatValidator

	hash hash.Hash

	checksum []byte

	maxPayloadSz uint64 // network config

	payloadSz uint64 // payload size of the streaming object from header

	writtenPayload uint64 // number of already written payload bytes
}

// errors related to invalid payload size
var (
	errExceedingMaxSize = errors.New("payload size is greater than the limit")
	errWrongPayloadSize = errors.New("wrong payload size")
)

func (t *validatingTarget) WriteHeader(obj *object.RawObject) error {
	t.payloadSz = obj.PayloadSize()
	chunkLn := uint64(len(obj.Payload()))

	// check chunk size
	if chunkLn > t.payloadSz {
		return errWrongPayloadSize
	}

	// check payload size limit
	if t.payloadSz > t.maxPayloadSz {
		return errExceedingMaxSize
	}

	cs := obj.PayloadChecksum()
	switch typ := cs.Type(); typ {
	default:
		return fmt.Errorf("(%T) unsupported payload checksum type %v", t, typ)
	case checksum.SHA256:
		t.hash = sha256.New()
	case checksum.TZ:
		t.hash = tz.New()
	}

	t.checksum = cs.Sum()

	if err := t.fmt.Validate(obj.Object()); err != nil {
		return fmt.Errorf("(%T) coult not validate object format: %w", t, err)
	}

	err := t.nextTarget.WriteHeader(obj)
	if err != nil {
		return err
	}

	// update written bytes
	//
	// Note: we MUST NOT add obj.PayloadSize() since obj
	// can carry only the chunk of the full payload
	t.writtenPayload += chunkLn

	return nil
}

func (t *validatingTarget) Write(p []byte) (n int, err error) {
	chunkLn := uint64(len(p))

	// check if new chunk will overflow payload size
	if t.writtenPayload+chunkLn > t.payloadSz {
		return 0, errWrongPayloadSize
	}

	_, err = t.hash.Write(p)
	if err != nil {
		return
	}

	n, err = t.nextTarget.Write(p)
	if err == nil {
		t.writtenPayload += uint64(n)
	}

	return
}

func (t *validatingTarget) Close() (*transformer.AccessIdentifiers, error) {
	// check payload size correctness
	if t.payloadSz != t.writtenPayload {
		return nil, errWrongPayloadSize
	}

	if !bytes.Equal(t.hash.Sum(nil), t.checksum) {
		return nil, fmt.Errorf("(%T) incorrect payload checksum", t)
	}

	return t.nextTarget.Close()
}
