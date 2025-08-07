package internal

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type commonPrm struct {
	cli coreclient.Client

	ctx context.Context

	signer user.Signer

	tokenSession *session.Object

	tokenBearer *bearer.Token

	local bool

	xHeaders []string
}

// SetClient sets base client for NeoFS API communication.
//
// Required parameter.
func (x *commonPrm) SetClient(cli coreclient.Client) {
	x.cli = cli
}

// SetContext sets context.Context for network communication.
//
// Required parameter.
func (x *commonPrm) SetContext(ctx context.Context) {
	x.ctx = ctx
}

// SetPrivateKey sets private key to sign the request(s).
//
// Required parameter.
func (x *commonPrm) SetPrivateKey(key *ecdsa.PrivateKey) {
	x.signer = user.NewAutoIDSigner(*key)
}

// SetSessionToken sets token of the session within which request should be sent.
//
// By default the request will be sent outside the session.
func (x *commonPrm) SetSessionToken(tok *session.Object) {
	x.tokenSession = tok
}

// SetBearerToken sets bearer token to be attached to the request.
//
// By default token is not attached to the request.
func (x *commonPrm) SetBearerToken(tok *bearer.Token) {
	x.tokenBearer = tok
}

// SetTTL sets time-to-live call option.
func (x *commonPrm) SetTTL(ttl uint32) {
	x.local = ttl < 2
}

// SetXHeaders sets request X-Headers.
//
// By default X-Headers will  not be attached to the request.
func (x *commonPrm) SetXHeaders(hs []string) {
	x.xHeaders = hs
}

type readPrmCommon struct {
	commonPrm
}

// PutObjectPrm groups parameters of PutObject operation.
type PutObjectPrm struct {
	commonPrm

	obj *object.Object
}

// SetObject sets object to be stored.
//
// Required parameter.
func (x *PutObjectPrm) SetObject(obj *object.Object) {
	x.obj = obj
}

// PutObjectRes groups the resulting values of PutObject operation.
type PutObjectRes struct {
	id oid.ID
}

// ID returns identifier of the stored object.
func (x PutObjectRes) ID() oid.ID {
	return x.id
}

// PutObject saves the object in local storage of the remote node.
//
// Client, context and key must be set.
//
// Returns any error which prevented the operation from completing correctly in error return.
func PutObject(prm PutObjectPrm) (*PutObjectRes, error) {
	var prmCli client.PrmObjectPutInit

	prmCli.MarkLocal()

	if prm.tokenSession != nil {
		prmCli.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prmCli.WithBearerToken(*prm.tokenBearer)
	}

	prmCli.WithXHeaders(prm.xHeaders...)

	w, err := prm.cli.ObjectPutInit(prm.ctx, *prm.obj, prm.signer, prmCli)
	if err != nil {
		return nil, fmt.Errorf("init object writing on client: %w", err)
	}

	_, err = w.Write(prm.obj.Payload())
	if err != nil {
		return nil, fmt.Errorf("write object payload into stream: %w", err)
	}

	err = w.Close()
	if err != nil {
		ReportError(prm.cli, err)
		return nil, fmt.Errorf("finish object stream: %w", err)
	}

	return &PutObjectRes{
		id: w.GetResult().StoredObjectID(),
	}, nil
}

// SearchObjectsPrm groups parameters of SearchObjects operation.
type SearchObjectsPrm struct {
	readPrmCommon

	cid    cid.ID
	cliPrm client.PrmObjectSearch
}

// SetContainerID sets identifier of the container to search the objects.
//
// Required parameter.
func (x *SearchObjectsPrm) SetContainerID(id cid.ID) {
	x.cid = id
}

// SetFilters sets search filters.
func (x *SearchObjectsPrm) SetFilters(fs object.SearchFilters) {
	x.cliPrm.SetFilters(fs)
}

// SearchObjectsRes groups the resulting values of SearchObjects operation.
type SearchObjectsRes struct {
	ids []oid.ID
}

// IDList returns identifiers of the matched objects.
func (x SearchObjectsRes) IDList() []oid.ID {
	return x.ids
}

// SearchObjects selects objects from container which match the filters.
//
// Returns any error which prevented the operation from completing correctly in error return.
func SearchObjects(prm SearchObjectsPrm) (*SearchObjectsRes, error) {
	if prm.local {
		prm.cliPrm.MarkLocal()
	}

	if prm.tokenSession != nil {
		prm.cliPrm.WithinSession(*prm.tokenSession)
	}

	if prm.tokenBearer != nil {
		prm.cliPrm.WithBearerToken(*prm.tokenBearer)
	}

	prm.cliPrm.WithXHeaders(prm.xHeaders...)

	rdr, err := prm.cli.ObjectSearchInit(prm.ctx, prm.cid, prm.signer, prm.cliPrm)
	if err != nil {
		return nil, fmt.Errorf("init object searching in client: %w", err)
	}

	var ids []oid.ID

	err = rdr.Iterate(func(id oid.ID) bool {
		ids = append(ids, id)
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("search objects using NeoFS API: %w", err)
	}

	return &SearchObjectsRes{
		ids: ids,
	}, nil
}
