package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// PutObjectPrm groups parameters of PutObject operation.
type PutObjectPrm struct {
	commonObjectPrm

	hdr *object.Object

	rdr io.Reader

	headerCallback func(*object.Object)
}

// SetHeader sets object header.
func (x *PutObjectPrm) SetHeader(hdr *object.Object) {
	x.hdr = hdr
}

// SetPayloadReader sets reader of the object payload.
func (x *PutObjectPrm) SetPayloadReader(rdr io.Reader) {
	x.rdr = rdr
}

// SetHeaderCallback sets callback which is called on the object after the header is received
// but before the payload is written.
func (x *PutObjectPrm) SetHeaderCallback(f func(*object.Object)) {
	x.headerCallback = f
}

// PutObjectRes groups the resulting values of PutObject operation.
type PutObjectRes struct {
	id oid.ID
}

// ID returns identifier of the created object.
func (x PutObjectRes) ID() oid.ID {
	return x.id
}

// PutObject saves the object in NeoFS network.
//
// Returns any error which prevented the operation from completing correctly in error return.
func PutObject(ctx context.Context, prm PutObjectPrm) (*PutObjectRes, error) {
	var putPrm client.PrmObjectPutInit

	if prm.sessionToken != nil {
		putPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		putPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.local {
		putPrm.MarkLocal()
	}

	putPrm.WithXHeaders(prm.xHeaders...)

	wrt, err := prm.cli.ObjectPutInit(ctx, *prm.hdr, prm.signer, putPrm)
	if err != nil {
		return nil, fmt.Errorf("init object writing: %w", err)
	}

	if prm.headerCallback != nil {
		prm.headerCallback(prm.hdr)
	}

	sz := prm.hdr.PayloadSize()

	if data := prm.hdr.Payload(); len(data) > 0 {
		if prm.rdr != nil {
			prm.rdr = io.MultiReader(bytes.NewReader(data), prm.rdr)
		} else {
			prm.rdr = bytes.NewReader(data)
			sz = uint64(len(data))
		}
	}

	if prm.rdr != nil {
		const defaultBufferSizePut = 3 << 20 // Maximum chunk size is 3 MiB in the SDK.

		if sz == 0 || sz > defaultBufferSizePut {
			sz = defaultBufferSizePut
		}

		buf := make([]byte, sz)

		_, err = io.CopyBuffer(wrt, prm.rdr, buf)
		if err != nil {
			return nil, fmt.Errorf("copy data into object stream: %w", err)
		}
	}

	err = wrt.Close()
	if err != nil {
		return nil, fmt.Errorf("finish object stream: %w", err)
	}

	return &PutObjectRes{
		id: wrt.GetResult().StoredObjectID(),
	}, nil
}

// GetObjectPrm groups parameters of GetObject operation.
type GetObjectPrm struct {
	commonObjectPrm
	objectAddressPrm
	rawPrm
}

// GetObject reads an object by address.
//
// Interrupts on any writer error. If successful, payload is written to the writer.
//
// Returns any error which prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func GetObject(ctx context.Context, prm GetObjectPrm) (object.Object, io.Reader, error) {
	var getPrm client.PrmObjectGet

	if prm.sessionToken != nil {
		getPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		getPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.raw {
		getPrm.MarkRaw()
	}

	if prm.local {
		getPrm.MarkLocal()
	}

	getPrm.WithXHeaders(prm.xHeaders...)

	hdr, rdr, err := prm.cli.ObjectGetInit(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.signer, getPrm)
	if err != nil {
		return object.Object{}, nil, fmt.Errorf("init object reading on client: %w", err)
	}

	return hdr, rdr, nil
}
