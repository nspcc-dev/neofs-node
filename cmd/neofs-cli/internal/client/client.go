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
	payloadWriterPrm
	headerCallback func(*object.Object)
}

// SetHeaderCallback sets callback which is called on the object after the header is received
// but before the payload is written.
func (p *GetObjectPrm) SetHeaderCallback(f func(*object.Object)) {
	p.headerCallback = f
}

// GetObjectRes groups the resulting values of GetObject operation.
type GetObjectRes struct {
	hdr *object.Object
}

// Header returns the header of the request object.
func (x GetObjectRes) Header() *object.Object {
	return x.hdr
}

// GetObject reads an object by address.
//
// Interrupts on any writer error. If successful, payload is written to the writer.
//
// Returns any error which prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func GetObject(ctx context.Context, prm GetObjectPrm) (*GetObjectRes, error) {
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
		return nil, fmt.Errorf("init object reading on client: %w", err)
	}

	if prm.headerCallback != nil {
		prm.headerCallback(&hdr)
	}

	_, err = io.Copy(prm.wrt, rdr)
	if err != nil {
		return nil, fmt.Errorf("copy payload: %w", err)
	}

	return &GetObjectRes{
		hdr: &hdr,
	}, nil
}

// HashPayloadRangesPrm groups parameters of HashPayloadRanges operation.
type HashPayloadRangesPrm struct {
	commonObjectPrm
	objectAddressPrm

	tz bool

	rngs []*object.Range

	salt []byte
}

// TZ sets flag to request Tillich-Zemor hashes.
func (x *HashPayloadRangesPrm) TZ() {
	x.tz = true
}

// SetRanges sets a list of payload ranges to hash.
func (x *HashPayloadRangesPrm) SetRanges(rngs []*object.Range) {
	x.rngs = rngs
}

// SetSalt sets data for each range to be XOR'ed with.
func (x *HashPayloadRangesPrm) SetSalt(salt []byte) {
	x.salt = salt
}

// HashPayloadRangesRes groups the resulting values of HashPayloadRanges operation.
type HashPayloadRangesRes struct {
	cliRes [][]byte
}

// HashList returns a list of hashes of the payload ranges keeping order.
func (x HashPayloadRangesRes) HashList() [][]byte {
	return x.cliRes
}

// HashPayloadRanges requests hashes (by default SHA256) of the object payload ranges.
//
// Returns any error which prevented the operation from completing correctly in error return.
// Returns an error if number of received hashes differs with the number of requested ranges.
func HashPayloadRanges(ctx context.Context, prm HashPayloadRangesPrm) (*HashPayloadRangesRes, error) {
	var cliPrm client.PrmObjectHash

	if prm.local {
		cliPrm.MarkLocal()
	}

	cliPrm.UseSalt(prm.salt)

	rngs := make([]uint64, 2*len(prm.rngs))

	for i := range prm.rngs {
		rngs[2*i] = prm.rngs[i].GetOffset()
		rngs[2*i+1] = prm.rngs[i].GetLength()
	}

	cliPrm.SetRangeList(rngs...)

	if prm.tz {
		cliPrm.TillichZemorAlgo()
	}

	if prm.sessionToken != nil {
		cliPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		cliPrm.WithBearerToken(*prm.bearerToken)
	}

	cliPrm.WithXHeaders(prm.xHeaders...)

	res, err := prm.cli.ObjectHash(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("read payload hashes via client: %w", err)
	}

	return &HashPayloadRangesRes{
		cliRes: res,
	}, nil
}

// PayloadRangePrm groups parameters of PayloadRange operation.
type PayloadRangePrm struct {
	commonObjectPrm
	objectAddressPrm
	rawPrm
	payloadWriterPrm

	rng *object.Range
}

// SetRange sets payload range to read.
func (x *PayloadRangePrm) SetRange(rng *object.Range) {
	x.rng = rng
}

// PayloadRangeRes groups the resulting values of PayloadRange operation.
type PayloadRangeRes struct{}

// PayloadRange reads object payload range from NeoFS and writes it to the specified writer.
//
// Interrupts on any writer error.
//
// Returns any error which prevented the operation from completing correctly in error return.
// For raw reading, returns *object.SplitInfoError error if object is virtual.
func PayloadRange(ctx context.Context, prm PayloadRangePrm) (*PayloadRangeRes, error) {
	var cliPrm client.PrmObjectRange

	if prm.sessionToken != nil {
		cliPrm.WithinSession(*prm.sessionToken)
	}

	if prm.bearerToken != nil {
		cliPrm.WithBearerToken(*prm.bearerToken)
	}

	if prm.raw {
		cliPrm.MarkRaw()
	}

	if prm.local {
		cliPrm.MarkLocal()
	}

	cliPrm.WithXHeaders(prm.xHeaders...)

	rdr, err := prm.cli.ObjectRangeInit(ctx, prm.objAddr.Container(), prm.objAddr.Object(), prm.rng.GetOffset(), prm.rng.GetLength(), prm.signer, cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init payload reading: %w", err)
	}

	_, err = io.Copy(prm.wrt, rdr)
	if err != nil {
		return nil, fmt.Errorf("copy payload: %w", err)
	}

	return new(PayloadRangeRes), nil
}
