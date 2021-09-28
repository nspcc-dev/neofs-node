package getsvc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	sessionsdk "github.com/nspcc-dev/neofs-api-go/pkg/session"
	rpcclient "github.com/nspcc-dev/neofs-api-go/rpc/client"
	signature2 "github.com/nspcc-dev/neofs-api-go/util/signature"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/tzhash/tz"
)

var errWrongMessageSeq = errors.New("incorrect message sequence")

func (s *Service) toPrm(req *objectV2.GetRequest, stream objectSvc.GetObjectStream) (*getsvc.Prm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(sessionsdk.NewTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(getsvc.Prm)
	p.SetCommonParameters(commonPrm.
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetObjectWriter(&streamObjectWriter{stream})

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once

		p.SetRequestForwarder(groupAddressRequestForwarder(func(addr network.Address, c client.Client, pubkey []byte) (*objectSDK.Object, error) {
			var err error

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: think how to set the other fields
				metaHdr.SetOrigin(meta)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})

			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.GetObject implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go

			// open stream
			stream, err := rpc.GetObject(c.RawForAddress(addr), req, rpcclient.WithContext(stream.Context()))
			if err != nil {
				return nil, fmt.Errorf("stream opening failed: %w", err)
			}

			var (
				headWas bool
				payload []byte
				obj     = new(objectV2.Object)
				resp    = new(objectV2.GetResponse)
			)

			for {
				// receive message from server stream
				err := stream.Read(resp)
				if err != nil {
					if errors.Is(err, io.EOF) {
						if !headWas {
							return nil, io.ErrUnexpectedEOF
						}

						break
					}

					return nil, fmt.Errorf("reading the response failed: %w", err)
				}

				// verify response key
				if err = internal.VerifyResponseKeyV2(pubkey, resp); err != nil {
					return nil, err
				}

				// verify response structure
				if err := signature.VerifyServiceMessage(resp); err != nil {
					return nil, fmt.Errorf("response verification failed: %w", err)
				}

				switch v := resp.GetBody().GetObjectPart().(type) {
				default:
					return nil, fmt.Errorf("unexpected object part %T", v)
				case *objectV2.GetObjectPartInit:
					if headWas {
						return nil, errWrongMessageSeq
					}

					headWas = true

					obj.SetObjectID(v.GetObjectID())
					obj.SetSignature(v.GetSignature())

					hdr := v.GetHeader()
					obj.SetHeader(hdr)

					payload = make([]byte, 0, hdr.GetPayloadLength())
				case *objectV2.GetObjectPartChunk:
					if !headWas {
						return nil, errWrongMessageSeq
					}

					payload = append(payload, v.GetChunk()...)
				case *objectV2.SplitInfo:
					si := objectSDK.NewSplitInfoFromV2(v)
					return nil, objectSDK.NewSplitInfoError(si)
				}
			}

			obj.SetPayload(payload)

			// convert the object
			return objectSDK.NewFromV2(obj), nil
		}))
	}

	return p, nil
}

func (s *Service) toRangePrm(req *objectV2.GetRangeRequest, stream objectSvc.GetObjectRangeStream) (*getsvc.RangePrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(sessionsdk.NewTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangePrm)
	p.SetCommonParameters(commonPrm.
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetChunkWriter(&streamObjectRangeWriter{stream})
	p.SetRange(objectSDK.NewRangeFromV2(body.GetRange()))

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once

		p.SetRequestForwarder(groupAddressRequestForwarder(func(addr network.Address, c client.Client, pubkey []byte) (*objectSDK.Object, error) {
			var err error

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: think how to set the other fields
				metaHdr.SetOrigin(meta)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})

			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.ObjectPayloadRangeData implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go

			// open stream
			stream, err := rpc.GetObjectRange(c.RawForAddress(addr), req, rpcclient.WithContext(stream.Context()))
			if err != nil {
				return nil, fmt.Errorf("could not create Get payload range stream: %w", err)
			}

			payload := make([]byte, 0, body.GetRange().GetLength())

			resp := new(objectV2.GetRangeResponse)

			for {
				// receive message from server stream
				err := stream.Read(resp)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return nil, fmt.Errorf("reading the response failed: %w", err)
				}

				// verify response key
				if err = internal.VerifyResponseKeyV2(pubkey, resp); err != nil {
					return nil, err
				}

				// verify response structure
				if err := signature.VerifyServiceMessage(resp); err != nil {
					return nil, fmt.Errorf("could not verify %T: %w", resp, err)
				}

				switch v := resp.GetBody().GetRangePart().(type) {
				case nil:
					return nil, fmt.Errorf("unexpected range type %T", v)
				case *objectV2.GetRangePartChunk:
					payload = append(payload, v.GetChunk()...)
				case *objectV2.SplitInfo:
					si := objectSDK.NewSplitInfoFromV2(v)

					return nil, objectSDK.NewSplitInfoError(si)
				}
			}

			obj := objectSDK.NewRaw()
			obj.SetPayload(payload)

			return obj.Object(), nil
		}))
	}

	return p, nil
}

func (s *Service) toHashRangePrm(req *objectV2.GetRangeHashRequest) (*getsvc.RangeHashPrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(sessionsdk.NewTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangeHashPrm)
	p.SetCommonParameters(commonPrm.
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))

	rngsV2 := body.GetRanges()
	rngs := make([]*objectSDK.Range, 0, len(rngsV2))

	for i := range rngsV2 {
		rngs = append(rngs, objectSDK.NewRangeFromV2(rngsV2[i]))
	}

	p.SetRangeList(rngs)
	p.SetSalt(body.GetSalt())

	switch t := body.GetType(); t {
	default:
		return nil, fmt.Errorf("unknown checksum type %v", t)
	case refs.SHA256:
		p.SetHashGenerator(func() hash.Hash {
			return sha256.New()
		})
	case refs.TillichZemor:
		p.SetHashGenerator(func() hash.Hash {
			return tz.New()
		})
	}

	return p, nil
}

type headResponseWriter struct {
	mainOnly bool

	body *objectV2.HeadResponseBody
}

func (w *headResponseWriter) WriteHeader(hdr *object.Object) error {
	if w.mainOnly {
		w.body.SetHeaderPart(toShortObjectHeader(hdr))
	} else {
		w.body.SetHeaderPart(toFullObjectHeader(hdr))
	}

	return nil
}

func (s *Service) toHeadPrm(ctx context.Context, req *objectV2.HeadRequest, resp *objectV2.HeadResponse) (*getsvc.HeadPrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(sessionsdk.NewTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(getsvc.HeadPrm)
	p.SetCommonParameters(commonPrm.
		WithPrivateKey(key),
	)

	body := req.GetBody()
	p.WithAddress(objectSDK.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetHeaderWriter(&headResponseWriter{
		mainOnly: body.GetMainOnly(),
		body:     resp.GetBody(),
	})

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once

		p.SetRequestForwarder(groupAddressRequestForwarder(func(addr network.Address, c client.Client, pubkey []byte) (*objectSDK.Object, error) {
			var err error

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: think how to set the other fields
				metaHdr.SetOrigin(meta)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})

			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.GetObjectHeader implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go

			// send Head request
			resp, err := rpc.HeadObject(c.RawForAddress(addr), req, rpcclient.WithContext(ctx))
			if err != nil {
				return nil, fmt.Errorf("sending the request failed: %w", err)
			}

			// verify response key
			if err = internal.VerifyResponseKeyV2(pubkey, resp); err != nil {
				return nil, err
			}

			// verify response structure
			if err := signature.VerifyServiceMessage(resp); err != nil {
				return nil, fmt.Errorf("response verification failed: %w", err)
			}

			var (
				hdr   *objectV2.Header
				idSig *refs.Signature
			)

			switch v := resp.GetBody().GetHeaderPart().(type) {
			case nil:
				return nil, fmt.Errorf("unexpected header type %T", v)
			case *objectV2.ShortHeader:
				if !body.GetMainOnly() {
					return nil, fmt.Errorf("wrong header part type: expected %T, received %T",
						(*objectV2.ShortHeader)(nil), (*objectV2.HeaderWithSignature)(nil),
					)
				}

				h := v

				hdr = new(objectV2.Header)
				hdr.SetPayloadLength(h.GetPayloadLength())
				hdr.SetVersion(h.GetVersion())
				hdr.SetOwnerID(h.GetOwnerID())
				hdr.SetObjectType(h.GetObjectType())
				hdr.SetCreationEpoch(h.GetCreationEpoch())
				hdr.SetPayloadHash(h.GetPayloadHash())
				hdr.SetHomomorphicHash(h.GetHomomorphicHash())
			case *objectV2.HeaderWithSignature:
				if body.GetMainOnly() {
					return nil, fmt.Errorf("wrong header part type: expected %T, received %T",
						(*objectV2.HeaderWithSignature)(nil), (*objectV2.ShortHeader)(nil),
					)
				}

				hdrWithSig := v
				if hdrWithSig == nil {
					return nil, errors.New("nil object part")
				}

				hdr = hdrWithSig.GetHeader()
				idSig = hdrWithSig.GetSignature()

				if err := signature2.VerifyDataWithSource(
					signature.StableMarshalerWrapper{
						SM: p.Address().ObjectID().ToV2(),
					},
					func() (key, sig []byte) {
						return idSig.GetKey(), idSig.GetSign()
					},
				); err != nil {
					return nil, fmt.Errorf("incorrect object header signature: %w", err)
				}
			case *objectV2.SplitInfo:
				si := objectSDK.NewSplitInfoFromV2(v)

				return nil, objectSDK.NewSplitInfoError(si)
			}

			obj := new(objectV2.Object)
			obj.SetHeader(hdr)
			obj.SetSignature(idSig)

			raw := object.NewRawFromV2(obj)
			raw.SetID(p.Address().ObjectID())

			// convert the object
			return raw.Object().SDK(), nil
		}))
	}

	return p, nil
}

func splitInfoResponse(info *objectSDK.SplitInfo) *objectV2.GetResponse {
	resp := new(objectV2.GetResponse)

	body := new(objectV2.GetResponseBody)
	resp.SetBody(body)

	body.SetObjectPart(info.ToV2())

	return resp
}

func splitInfoRangeResponse(info *objectSDK.SplitInfo) *objectV2.GetRangeResponse {
	resp := new(objectV2.GetRangeResponse)

	body := new(objectV2.GetRangeResponseBody)
	resp.SetBody(body)

	body.SetRangePart(info.ToV2())

	return resp
}

func setSplitInfoHeadResponse(info *objectSDK.SplitInfo, resp *objectV2.HeadResponse) {
	resp.GetBody().SetHeaderPart(info.ToV2())
}

func toHashResponse(typ refs.ChecksumType, res *getsvc.RangeHashRes) *objectV2.GetRangeHashResponse {
	resp := new(objectV2.GetRangeHashResponse)

	body := new(objectV2.GetRangeHashResponseBody)
	resp.SetBody(body)

	body.SetType(typ)
	body.SetHashList(res.Hashes())

	return resp
}

func toFullObjectHeader(hdr *object.Object) objectV2.GetHeaderPart {
	obj := hdr.ToV2()

	hs := new(objectV2.HeaderWithSignature)
	hs.SetHeader(obj.GetHeader())
	hs.SetSignature(obj.GetSignature())

	return hs
}

func toShortObjectHeader(hdr *object.Object) objectV2.GetHeaderPart {
	hdrV2 := hdr.ToV2().GetHeader()

	sh := new(objectV2.ShortHeader)
	sh.SetOwnerID(hdrV2.GetOwnerID())
	sh.SetCreationEpoch(hdrV2.GetCreationEpoch())
	sh.SetPayloadLength(hdrV2.GetPayloadLength())
	sh.SetVersion(hdrV2.GetVersion())
	sh.SetObjectType(hdrV2.GetObjectType())
	sh.SetHomomorphicHash(hdrV2.GetHomomorphicHash())
	sh.SetPayloadHash(hdrV2.GetPayloadHash())

	return sh
}

func groupAddressRequestForwarder(f func(network.Address, client.Client, []byte) (*objectSDK.Object, error)) getsvc.RequestForwarder {
	return func(info client.NodeInfo, c client.Client) (*objectSDK.Object, error) {
		var (
			firstErr error
			res      *objectSDK.Object

			key = info.PublicKey()
		)

		info.AddressGroup().IterateAddresses(func(addr network.Address) (stop bool) {
			var err error

			defer func() {
				stop = err == nil

				if stop || firstErr == nil {
					firstErr = err
				}

				// would be nice to log otherwise
			}()

			res, err = f(addr, c, key)

			return
		})

		return res, firstErr
	}
}
