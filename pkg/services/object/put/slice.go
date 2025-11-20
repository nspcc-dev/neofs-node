package putsvc

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/object/slicer"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type slicingTarget struct {
	ctx              context.Context
	signer           user.Signer
	sessionToken     *session.Object
	currentEpoch     uint64
	maxObjSize       uint64
	homoHashDisabled bool

	nextTarget internal.Target

	payloadWriter *slicer.PayloadWriter
}

// returns [internal.Target] for raw root object streamed by the client
// with payload slicing and child objects' formatting. Each ready child object
// is written into destination target constructed via the given [internal.Target].
func newSlicingTarget(
	ctx context.Context,
	maxObjSize uint64,
	homoHashDisabled bool,
	signer user.Signer,
	sessionToken *session.Object,
	curEpoch uint64,
	initNextTarget internal.Target,
) internal.Target {
	return &slicingTarget{
		ctx:              ctx,
		signer:           signer,
		sessionToken:     sessionToken,
		currentEpoch:     curEpoch,
		maxObjSize:       maxObjSize,
		homoHashDisabled: homoHashDisabled,
		nextTarget:       initNextTarget,
	}
}

func (x *slicingTarget) WriteHeader(hdr *object.Object) error {
	var opts slicer.Options
	opts.SetObjectPayloadLimit(x.maxObjSize)
	opts.SetCurrentNeoFSEpoch(x.currentEpoch)
	if x.sessionToken != nil {
		opts.SetSession(*x.sessionToken)
	}
	if !x.homoHashDisabled {
		opts.CalculateHomomorphicChecksum()
	}

	if payloadSize := hdr.PayloadSize(); payloadSize != 0 && payloadSize != math.MaxUint64 {
		// https://github.com/nspcc-dev/neofs-api/blob/d95228c40283cf6e188073a87a802af7e5dc0a7d/object/types.proto#L93-L95
		// zero may be explicitly set and be true, but node previously considered zero
		// value as unknown payload, so we keep this behavior for now
		opts.SetPayloadSize(payloadSize)
	}

	var err error
	x.payloadWriter, err = slicer.InitPut(x.ctx, &readyObjectWriter{
		nextTarget: x.nextTarget,
	}, *hdr, x.signer, opts)
	if err != nil {
		return fmt.Errorf("init object slicer: %w", err)
	}

	return nil
}

func (x *slicingTarget) Write(p []byte) (n int, err error) {
	return x.payloadWriter.Write(p)
}

func (x *slicingTarget) Close() (oid.ID, error) {
	err := x.payloadWriter.Close()
	if err != nil && !errors.Is(err, apistatus.ErrIncomplete) {
		return oid.ID{}, fmt.Errorf("finish object slicing: %w", err)
	}

	return x.payloadWriter.ID(), err
}

// implements slicer.ObjectWriter for ready child objects.
type readyObjectWriter struct {
	nextTarget internal.Target
}

func (x *readyObjectWriter) ObjectPutInit(_ context.Context, hdr object.Object, _ user.Signer, _ client.PrmObjectPutInit) (client.ObjectWriter, error) {
	err := x.nextTarget.WriteHeader(&hdr)
	if err != nil {
		return nil, err
	}

	return &readyObjectPayloadWriter{
		target: x.nextTarget,
	}, nil
}

// implements client.ObjectWriter for ready child objects.
type readyObjectPayloadWriter struct {
	target internal.Target
}

func (x *readyObjectPayloadWriter) Write(p []byte) (int, error) {
	return x.target.Write(p)
}

func (x *readyObjectPayloadWriter) Close() error {
	_, err := x.target.Close()
	return err
}

func (x *readyObjectPayloadWriter) GetResult() (res client.ResObjectPut) {
	// FIXME: client.ResObjectPut is private, at the same time, slicer doesn't call
	// this method (now)
	return client.ResObjectPut{}
}
