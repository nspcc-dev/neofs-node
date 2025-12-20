package putsvc

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/tzhash/tz"
	"go.uber.org/zap"
)

// validatingTarget validates object format and content.
type validatingTarget struct {
	l *zap.Logger

	nextTarget internal.Target

	fmt *objectcore.FormatValidator

	unpreparedObject bool

	hash hash.Hash

	checksum []byte

	quotaLimiter    QuotaLimiter
	maxPayloadSz    uint64 // network config
	payloadSz       uint64 // payload size of the streaming object from header
	writtenPayload  uint64 // number of already written payload bytes
	cachedHeader    *object.Object
	cachedCnr       container.Container
	isECPart        bool
	cachedRepNumber uint64
	cachedECRules   []netmap.ECRule

	homomorphicChecksumRequired bool
}

var (
	// ErrExceedingMaxSize is returned when payload size is greater than the limit.
	ErrExceedingMaxSize = errors.New("payload size is greater than the limit")
	// ErrWrongPayloadSize is returned when chunk payload size is greater than the length declared in header.
	ErrWrongPayloadSize = errors.New("wrong payload size")
)

func (t *validatingTarget) WriteHeader(obj *object.Object) error {
	t.cachedHeader = obj
	t.payloadSz = obj.PayloadSize()
	chunkLn := uint64(len(obj.Payload()))
	p := t.cachedCnr.PlacementPolicy()
	for i := range p.NumberOfReplicas() {
		// simplification: there can be duplicates in complex cases with
		// multiple placement vectors, but considering placement
		// ambiguity, the rarity of complex placement rules, and quotas
		// caching, this is an acceptable prediction
		t.cachedRepNumber += uint64(p.ReplicaNumberByIndex(i))
	}
	t.cachedECRules = p.ECRules()

	if !t.unpreparedObject {
		// check chunk size
		if chunkLn > t.payloadSz {
			return ErrWrongPayloadSize
		}

		// check payload size limit
		if t.payloadSz > t.maxPayloadSz {
			return ErrExceedingMaxSize
		}

		if t.homomorphicChecksumRequired {
			cs, csSet := obj.PayloadHomomorphicHash()
			switch {
			case !csSet:
				return errors.New("missing homomorphic payload checksum")
			case cs.Type() != checksum.TillichZemor:
				return fmt.Errorf("wrong/unsupported type of homomorphic payload checksum: %s instead of %s",
					cs.Type(), checksum.TillichZemor)
			case len(cs.Value()) != tz.Size:
				return fmt.Errorf("invalid/unsupported length of %s homomorphic payload checksum: %d instead of %d",
					cs.Type(), len(cs.Value()), tz.Size)
			}
		}

		cs, csSet := obj.PayloadChecksum()
		if !csSet {
			return errors.New("missing payload checksum")
		}

		switch typ := cs.Type(); typ {
		default:
			return fmt.Errorf("(%T) unsupported payload checksum type %v", t, typ)
		case checksum.SHA256:
			t.hash = sha256.New()
		case checksum.TillichZemor:
			t.hash = tz.New()
		}

		t.checksum = cs.Value()
	}

	if err := t.fmt.Validate(obj, t.unpreparedObject); err != nil {
		return fmt.Errorf("(%T) could not validate object format: %w", t, err)
	}

	err := t.checkQuotaLimits(obj, t.payloadSz)
	if err != nil {
		return err
	}

	err = t.nextTarget.WriteHeader(obj)
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

	if !t.unpreparedObject {
		// check if new chunk will overflow payload size
		if t.writtenPayload+chunkLn > t.payloadSz {
			return 0, ErrWrongPayloadSize
		}

		_, err = t.hash.Write(p)
		if err != nil {
			return
		}
	}

	n, err = t.nextTarget.Write(p)
	if err == nil {
		t.writtenPayload += uint64(n)
	}

	err = t.checkQuotaLimits(t.cachedHeader, t.writtenPayload)

	return
}

func (t *validatingTarget) Close() (oid.ID, error) {
	if !t.unpreparedObject {
		// check payload size correctness
		if t.payloadSz != t.writtenPayload {
			return oid.ID{}, ErrWrongPayloadSize
		}

		if !bytes.Equal(t.hash.Sum(nil), t.checksum) {
			return oid.ID{}, fmt.Errorf("(%T) incorrect payload checksum", t)
		}
	}

	return t.nextTarget.Close()
}

func (t *validatingTarget) checkQuotaLimits(obj *object.Object, written uint64) error {
	// header validation ensured values are non-zero before
	cID := obj.GetContainerID()
	owner := obj.Owner()
	softLeft, hardLeft, err := t.quotaLimiter.AvailableQuotasLeft(cID, owner)
	if err != nil {
		t.l.Warn("failed to check quotas, accept object anyway",
			zap.Stringer("cID", cID),
			zap.Stringer("owner", owner),
			zap.Error(err))
	} else {
		var needed uint64
		if t.isECPart {
			needed = written
		} else {
			needed = written * t.cachedRepNumber

			if t.unpreparedObject {
				for i := range t.cachedECRules {
					dataPartNum := uint64(t.cachedECRules[i].DataPartNum())
					partLen := (written + dataPartNum - 1) / dataPartNum
					needed += (dataPartNum + uint64(t.cachedECRules[i].ParityPartNum())) * partLen
				}
			}
		}

		if needed > softLeft {
			t.l.Warn("soft quota limit has been reached",
				zap.Stringer("cID", cID),
				zap.Stringer("owner", owner),
				zap.Error(apistatus.ErrQuotaExceeded))
		}

		if needed > hardLeft {
			return apistatus.ErrQuotaExceeded
		}
	}

	return nil
}
