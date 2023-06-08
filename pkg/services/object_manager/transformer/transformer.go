package transformer

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	slicerSDK "github.com/nspcc-dev/neofs-sdk-go/object/slicer"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/tzhash/tz"
)

type payloadSizeLimiter struct {
	maxSize                uint64
	withoutHomomorphicHash bool
	signer                 neofscrypto.Signer
	sessionToken           *session.Object
	networkState           netmap.State

	stream     *slicerSDK.PayloadWriter
	objSlicer  *slicerSDK.Slicer
	targetInit TargetInitializer

	_changedParentID *oid.ID
}

// objStreamInitializer implements [slicerSDK.ObjectWriter].
type objStreamInitializer struct {
	targetInit TargetInitializer

	_psl     *payloadSizeLimiter
	_signer  neofscrypto.Signer
	_objType object.Type
}

var (
	_emptyPayloadSHA256Sum = sha256.Sum256(nil)
	_emptyPayloadTZSum     = tz.Sum(nil)
)

func (o *objStreamInitializer) InitDataStream(header object.Object) (io.Writer, error) {
	stream := o.targetInit()
	linkObj := len(header.Children()) > 0

	// v1.0.0-rc.8 has a bug that does not allow any non-regular objects
	// to be split, thus that check, see https://github.com/nspcc-dev/neofs-sdk-go/issues/442.
	if o._objType != object.TypeRegular {
		if header.SplitID() != nil {
			// non-regular object has been split;
			// needed to make it carefully and add
			// original object type to the parent
			// header only
			if par := header.Parent(); par != nil {
				par.SetType(o._objType)
				err := _healHeader(o._signer, par)
				if err != nil {
					return nil, fmt.Errorf("broken non-regular object (parent): %w", err)
				}

				newID, _ := par.ID()
				o._psl._changedParentID = &newID

				header.SetParent(par)

				// linking objects will be healed
				// below anyway
				if !linkObj {
					err = _healHeader(o._signer, &header)
					if err != nil {
						return nil, fmt.Errorf("broken non-regular object (child): %w", err)
					}
				}
			}
		} else {
			// non-regular object has not been split
			// so just restore its type
			header.SetType(o._objType)
			err := _healHeader(o._signer, &header)
			if err != nil {
				return nil, fmt.Errorf("broken non-regular object: %w", err)
			}

			newID, _ := header.ID()
			o._psl._changedParentID = &newID
		}
	}

	// v1.0.0-rc.8 has a bug that relates linking objects, thus that
	// check, see https://github.com/nspcc-dev/neofs-sdk-go/pull/427.
	if linkObj {
		header.SetPayloadSize(0)

		var cs checksum.Checksum
		cs.SetSHA256(_emptyPayloadSHA256Sum)

		header.SetPayloadChecksum(cs)

		_, set := header.PayloadHomomorphicHash()
		if set {
			cs.SetTillichZemor(_emptyPayloadTZSum)
			header.SetPayloadHomomorphicHash(cs)
		}

		err := _healHeader(o._signer, &header)
		if err != nil {
			return nil, fmt.Errorf("broken linking object: %w", err)
		}
	}

	err := stream.WriteHeader(&header)
	if err != nil {
		return nil, err
	}

	return &objStream{target: stream, _linkObj: linkObj}, nil
}

// _healHeader recalculates all signature related fields that are
// broken after any setter call.
func _healHeader(signer neofscrypto.Signer, header *object.Object) error {
	err := object.CalculateAndSetID(header)
	if err != nil {
		return fmt.Errorf("id recalculation: %w", err)
	}

	err = object.CalculateAndSetSignature(signer, header)
	if err != nil {
		return fmt.Errorf("signature recalculation: %w", err)
	}

	return nil
}

// objStream implements [io.Writer] and [io.Closer].
type objStream struct {
	target ObjectTarget

	_linkObj bool
}

func (o *objStream) Write(p []byte) (n int, err error) {
	emptyPayload := len(p) == 0
	if emptyPayload {
		return 0, nil
	}

	if o._linkObj && !emptyPayload {
		return 0, errors.New("linking object with payload")
	}

	return o.target.Write(p)
}

func (o *objStream) Close() error {
	_, err := o.target.Close()
	return err
}

// NewPayloadSizeLimiter returns ObjectTarget instance that restricts payload length
// of the writing object and writes generated objects to targets from initializer.
//
// Calculates and adds homomorphic hash to resulting objects only if withoutHomomorphicHash
// is false.
//
// Objects w/ payload size less or equal than max size remain untouched.
func NewPayloadSizeLimiter(maxSize uint64, withoutHomomorphicHash bool, signer neofscrypto.Signer,
	sToken *session.Object, nState netmap.State, nextTargetInit TargetInitializer) ObjectTarget {
	return &payloadSizeLimiter{
		maxSize:                maxSize,
		withoutHomomorphicHash: withoutHomomorphicHash,
		signer:                 signer,
		sessionToken:           sToken,
		networkState:           nState,
		targetInit:             nextTargetInit,
	}
}

func (s *payloadSizeLimiter) WriteHeader(hdr *object.Object) error {
	var opts slicerSDK.Options
	opts.SetObjectPayloadLimit(s.maxSize)
	opts.SetCurrentNeoFSEpoch(s.networkState.CurrentEpoch())
	if !s.withoutHomomorphicHash {
		opts.CalculateHomomorphicChecksum()
	}

	cid, _ := hdr.ContainerID()
	streamInitializer := &objStreamInitializer{
		targetInit: s.targetInit,
		_psl:       s,
		_signer:    s.signer,
		_objType:   hdr.Type(),
	}

	if s.sessionToken == nil {
		s.objSlicer = slicerSDK.New(s.signer, cid, *hdr.OwnerID(), streamInitializer, opts)
	} else {
		s.objSlicer = slicerSDK.NewSession(s.signer, cid, *s.sessionToken, streamInitializer, opts)
	}

	var attrs []string
	if oAttrs := hdr.Attributes(); len(oAttrs) > 0 {
		attrs = make([]string, 0, len(oAttrs)*2)

		for _, a := range oAttrs {
			attrs = append(attrs, a.Key(), a.Value())
		}
	}

	var err error
	s.stream, err = s.objSlicer.InitPayloadStream(attrs...)
	if err != nil {
		return fmt.Errorf("initializing payload stream: %w", err)
	}

	return nil
}

func (s *payloadSizeLimiter) Write(p []byte) (int, error) {
	return s.stream.Write(p)
}

func (s *payloadSizeLimiter) Close() (*AccessIdentifiers, error) {
	err := s.stream.Close()
	if err != nil {
		return nil, err
	}

	id := s.stream.ID()

	ids := new(AccessIdentifiers)
	ids.WithSelfID(id)

	// object's header has been changed therefore SDK `Slicer`
	// returned the broken ID, let's help it and correct the ID
	if s._changedParentID != nil {
		ids.WithSelfID(*s._changedParentID)
	}

	return ids, nil
}
