package transformer

import (
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	slicerSDK "github.com/nspcc-dev/neofs-sdk-go/object/slicer"
	"github.com/nspcc-dev/neofs-sdk-go/session"
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
}

func (o *objStreamInitializer) InitDataStream(header object.Object) (io.Writer, error) {
	stream := o.targetInit()

	err := stream.WriteHeader(&header)
	if err != nil {
		return nil, err
	}

	return &objStream{stream}, nil
}

// objStream implements [io.Writer] and [io.Closer].
type objStream struct {
	target ObjectTarget
}

func (o *objStream) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
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

	if s.sessionToken == nil {
		s.objSlicer = slicerSDK.New(s.signer, cid, *hdr.OwnerID(), &objStreamInitializer{s.targetInit}, opts)
	} else {
		s.objSlicer = slicerSDK.NewSession(s.signer, cid, *s.sessionToken, &objStreamInitializer{s.targetInit}, opts)
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
