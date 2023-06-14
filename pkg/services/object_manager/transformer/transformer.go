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

	_changedParentID         *oid.ID
	_objectStreamInitializer *objStreamInitializer
}

// objStreamInitializer implements [slicerSDK.ObjectWriter].
type objStreamInitializer struct {
	targetInit TargetInitializer

	_psl      *payloadSizeLimiter
	_signer   neofscrypto.Signer
	_objType  object.Type
	_objBuf   *object.Object
	_splitID  *object.SplitID
	_childIDs []oid.ID
	_prev     *oid.ID
}

var (
	_emptyPayloadSHA256Sum = sha256.Sum256(nil)
	_emptyPayloadTZSum     = tz.Sum(nil)
)

func (o *objStreamInitializer) InitDataStream(header object.Object) (io.Writer, error) {
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
		header.SetChildren(o._childIDs...)

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

	// v1.0.0-rc.8 has a bug that breaks split field for the first child object,
	// thus that check, see https://github.com/nspcc-dev/neofs-sdk-go/issues/448.
	if o._objBuf == nil {
		if o._splitID == nil {
			// the first object, it is impossible to say
			// if there will be any others so cache it now
			// and generate split id for a potential object
			// chain
			o._objBuf = &header
			o._splitID = object.NewSplitID()

			return &_memoryObjStream{objInit: o}, nil
		}

		// not the first object, attach the missing split ID
		// and heal its header the second time; it is non-optimal
		// but the code here is already hard to read, and it
		// is full of kludges so let it be as stupid as possible

		header.SetSplitID(o._splitID)
		header.SetPreviousID(*o._prev)
		err := _healHeader(o._signer, &header)
		if err != nil {
			return nil, fmt.Errorf("broken intermediate object: %w", err)
		}

		id, _ := header.ID()
		o._childIDs = append(o._childIDs, id)
		o._prev = &id

		stream := o.targetInit()
		err = stream.WriteHeader(&header)
		if err != nil {
			return nil, fmt.Errorf("broken intermediate object: streaming header: %w", err)
		}

		return &objStream{target: stream, _linkObj: linkObj}, nil
	}

	// more objects are here it _is_ an object chain,
	// stream the cached one and continue chain handling

	// cached object streaming (`o._objBuf`)

	pl := o._objBuf.Payload()
	hdr := o._objBuf.CutPayload()
	hdr.SetSplitID(o._splitID)

	err := _healHeader(o._signer, hdr)
	if err != nil {
		return nil, fmt.Errorf("broken first child: %w", err)
	}

	id, _ := hdr.ID()
	o._childIDs = append(o._childIDs, id)

	stream := o.targetInit()

	err = stream.WriteHeader(hdr)
	if err != nil {
		return nil, fmt.Errorf("broken first child: cached header streaming: %w", err)
	}

	_, err = stream.Write(pl)
	if err != nil {
		return nil, fmt.Errorf("broken first child: cached payload streaming: %w", err)
	}

	_, err = stream.Close()
	if err != nil {
		return nil, fmt.Errorf("broken first child: stream for cached object closing: %w", err)
	}

	// mark the cached object as handled
	o._objBuf = nil

	// new object streaming (`header`)

	header.SetSplitID(o._splitID)
	header.SetPreviousID(id)
	err = _healHeader(o._signer, &header)
	if err != nil {
		return nil, fmt.Errorf("broken second child: %w", err)
	}

	id, _ = header.ID()
	o._childIDs = append(o._childIDs, id)
	o._prev = &id

	stream = o.targetInit()
	err = stream.WriteHeader(&header)
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

type _memoryObjStream struct {
	objInit *objStreamInitializer
}

func (m *_memoryObjStream) Write(p []byte) (n int, err error) {
	m.objInit._objBuf.SetPayload(append(m.objInit._objBuf.Payload(), p...))
	return len(p), nil
}

func (m *_memoryObjStream) Close() error {
	return nil
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

	s._objectStreamInitializer = streamInitializer

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

	if singleObj := s._objectStreamInitializer._objBuf; singleObj != nil {
		// we cached a single object (payload length has not exceeded
		// the limit) so stream it now without any changes

		stream := s.targetInit()
		pl := singleObj.Payload()
		hdr := singleObj.CutPayload()
		id, _ := hdr.ID()

		err = stream.WriteHeader(hdr)
		if err != nil {
			return nil, fmt.Errorf("single object: cached header streaming: %w", err)
		}

		_, err = stream.Write(pl)
		if err != nil {
			return nil, fmt.Errorf("single object: cached payload streaming: %w", err)
		}

		_, err = stream.Close()
		if err != nil {
			return nil, fmt.Errorf("single object: stream for cached object closing: %w", err)
		}

		ids := new(AccessIdentifiers)
		ids.WithSelfID(id)

		return ids, nil
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
