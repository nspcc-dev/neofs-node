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
}

// objStreamInitializer implements [slicerSDK.ObjectWriter].
type objStreamInitializer struct {
	targetInit TargetInitializer

	_signer neofscrypto.Signer
}

var (
	_emptyPayloadSHA256Sum = sha256.Sum256(nil)
	_emptyPayloadTZSum     = tz.Sum(nil)
)

func (o *objStreamInitializer) InitDataStream(header object.Object) (io.Writer, error) {
	stream := o.targetInit()

	// v1.0.0-rc.8 has a bug that relates linking objects, thus that
	// check, see https://github.com/nspcc-dev/neofs-sdk-go/pull/427.
	linkObj := len(header.Children()) > 0
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

		err := object.CalculateAndSetID(&header)
		if err != nil {
			return nil, fmt.Errorf("broken linking object id recalculation: %w", err)
		}

		err = object.CalculateAndSetSignature(o._signer, &header)
		if err != nil {
			return nil, fmt.Errorf("broken linking ojbect id recalculation: %w", err)
		}
	}

	err := stream.WriteHeader(&header)
	if err != nil {
		return nil, err
	}

	return &objStream{target: stream, _linkObj: linkObj}, nil
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
		_signer:    s.signer,
	}

	if s.sessionToken == nil {
		s.objSlicer = slicerSDK.New(s.signer, cid, *hdr.OwnerID(), streamInitializer, opts)
	} else {
		s.objSlicer = slicerSDK.NewSession(s.signer, cid, *s.sessionToken, streamInitializer, opts)
	}

	var err error
	s.stream, err = s.objSlicer.InitPayloadStream()
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

	return ids, nil
}
