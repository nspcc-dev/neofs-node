package putsvc

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

	initNextTarget transformer.TargetInitializer

	payloadWriter *slicer.PayloadWriter
}

// returns transformer.ObjectTarget for raw root object streamed by the client
// with payload slicing and child objects' formatting. Each ready child object
// is written into destination target constructed via the given transformer.TargetInitializer.
func newSlicingTarget(
	ctx context.Context,
	maxObjSize uint64,
	homoHashDisabled bool,
	signer user.Signer,
	sessionToken *session.Object,
	curEpoch uint64,
	initNextTarget transformer.TargetInitializer,
) transformer.ObjectTarget {
	return &slicingTarget{
		ctx:              ctx,
		signer:           signer,
		sessionToken:     sessionToken,
		currentEpoch:     curEpoch,
		maxObjSize:       maxObjSize,
		homoHashDisabled: homoHashDisabled,
		initNextTarget:   initNextTarget,
	}
}

func (x *slicingTarget) WriteHeader(hdr *object.Object) error {
	var opts slicer.Options
	opts.SetObjectPayloadLimit(x.maxObjSize)
	opts.SetCurrentNeoFSEpoch(x.currentEpoch)
	if x.sessionToken != nil {
		opts.SetSession(*x.sessionToken)
	}

	var err error
	x.payloadWriter, err = slicer.InitPut(x.ctx, &readyObjectWriter{
		initNextTarget: x.initNextTarget,
	}, *hdr, x.signer, opts)
	if err != nil {
		return fmt.Errorf("init object slicer: %w", err)
	}

	return nil
}

func (x *slicingTarget) Write(p []byte) (n int, err error) {
	return x.payloadWriter.Write(p)
}

func (x *slicingTarget) Close() (*transformer.AccessIdentifiers, error) {
	err := x.payloadWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("finish object slicing: %w", err)
	}

	return new(transformer.AccessIdentifiers).WithSelfID(x.payloadWriter.ID()), nil
}

// implements slicer.ObjectWriter for ready child objects.
type readyObjectWriter struct {
	initNextTarget transformer.TargetInitializer
}

func (x *readyObjectWriter) ObjectPutInit(_ context.Context, hdr object.Object, _ user.Signer, _ client.PrmObjectPutInit) (client.ObjectWriter, error) {
	tgt := x.initNextTarget()

	err := tgt.WriteHeader(&hdr)
	if err != nil {
		return nil, err
	}

	return &readyObjectPayloadWriter{
		target: tgt,
	}, nil
}

// implements client.ObjectWriter for ready child objects.
type readyObjectPayloadWriter struct {
	target transformer.ObjectTarget
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
