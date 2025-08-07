package internal

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// PutObjectPrm groups parameters of PutObject operation.
type PutObjectPrm struct {
	commonObjectPrm

	hdr *object.Object
}

// SetHeader sets object header.
func (x *PutObjectPrm) SetHeader(hdr *object.Object) {
	x.hdr = hdr
}

// PutObject saves the object in NeoFS network.
//
// Returns any error which prevented the operation from completing correctly in error return.
func PutObject(ctx context.Context, prm PutObjectPrm) (client.ObjectWriter, error) {
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

	return wrt, nil
}
