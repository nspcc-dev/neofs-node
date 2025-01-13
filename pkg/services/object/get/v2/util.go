package getsvc

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	protoobject "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	protorefs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/status"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"google.golang.org/grpc"
)

var errWrongMessageSeq = errors.New("incorrect message sequence")

func (s *Service) toPrm(req *objectV2.GetRequest, stream objectSvc.GetObjectStream) (*getsvc.Prm, error) {
	body := req.GetBody()

	addrV2 := body.GetAddress()
	if addrV2 == nil {
		return nil, errors.New("missing object address")
	}

	var addr oid.Address

	err := addr.ReadFromV2(*addrV2)
	if err != nil {
		return nil, fmt.Errorf("invalid object address: %w", err)
	}

	meta := req.GetMetaHeader()

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	streamWrapper := &streamObjectWriter{stream}

	p := new(getsvc.Prm)
	p.SetCommonParameters(commonPrm)

	p.WithAddress(addr)
	p.WithRawFlag(body.GetRaw())
	p.SetObjectWriter(streamWrapper)

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once

		var onceHeaderSending sync.Once
		var globalProgress int

		p.SetRequestForwarder(groupAddressRequestForwarder(func(ctx context.Context, addr network.Address, c client.MultiAddressClient, pubkey []byte) (*object.Object, error) {
			var err error

			key, err := s.keyStorage.GetKey(nil)
			if err != nil {
				return nil, err
			}

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: #1165 think how to set the other fields
				metaHdr.SetOrigin(meta)
				writeCurrentVersion(metaHdr)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})

			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.GetObject implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go

			// open stream
			var getStream protoobject.ObjectService_GetClient
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			err = c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
				getStream, err = protoobject.NewObjectServiceClient(conn).Get(ctx, req.ToGRPCMessage().(*protoobject.GetRequest))
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("stream opening failed: %w", err)
			}

			var (
				headWas       bool
				localProgress int
			)

			for {
				// receive message from server stream
				resp, err := getStream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						if !headWas {
							return nil, io.ErrUnexpectedEOF
						}

						break
					}

					internalclient.ReportError(c, err)
					return nil, fmt.Errorf("reading the response failed: %w", err)
				}

				// verify response key
				if err = internal.VerifyResponseKeyV2(pubkey, resp); err != nil {
					return nil, err
				}

				// verify response structure
				resp2 := new(objectV2.GetResponse)
				if err = resp2.FromGRPCMessage(resp); err != nil {
					panic(err) // can only fail on wrong type, here it's correct
				}
				if err := signature.VerifyServiceMessage(resp2); err != nil {
					return nil, fmt.Errorf("response verification failed: %w", err)
				}

				if err = checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
					return nil, err
				}

				switch v := resp.GetBody().GetObjectPart().(type) {
				default:
					return nil, fmt.Errorf("unexpected object part %T", v)
				case *protoobject.GetResponse_Body_Init_:
					if headWas {
						return nil, errWrongMessageSeq
					}

					headWas = true

					if v == nil || v.Init == nil {
						return nil, errors.New("nil header oneof field")
					}

					m := &protoobject.Object{
						ObjectId:  v.Init.ObjectId,
						Signature: v.Init.Signature,
						Header:    v.Init.Header,
					}

					obj := new(objectV2.Object)
					if err := obj.FromGRPCMessage(m); err != nil {
						panic(err) // can only fail on wrong type, here it's correct
					}

					onceHeaderSending.Do(func() {
						err = streamWrapper.WriteHeader(object.NewFromV2(obj))
					})
					if err != nil {
						return nil, fmt.Errorf("could not write object header in Get forwarder: %w", err)
					}
				case *protoobject.GetResponse_Body_Chunk:
					if !headWas {
						return nil, errWrongMessageSeq
					}

					origChunk := v.GetChunk()

					chunk := chunkToSend(globalProgress, localProgress, origChunk)
					if len(chunk) == 0 {
						localProgress += len(origChunk)
						continue
					}

					if err = streamWrapper.WriteChunk(chunk); err != nil {
						return nil, fmt.Errorf("could not write object chunk in Get forwarder: %w", err)
					}

					localProgress += len(origChunk)
					globalProgress += len(chunk)
				case *protoobject.GetResponse_Body_SplitInfo:
					if v == nil || v.SplitInfo == nil {
						return nil, errors.New("nil split info oneof field")
					}
					var si2 objectV2.SplitInfo
					if err := si2.FromGRPCMessage(v.SplitInfo); err != nil {
						panic(err) // can only fail on wrong type, here it's correct
					}
					si := object.NewSplitInfoFromV2(&si2)
					return nil, object.NewSplitInfoError(si)
				}
			}

			return nil, nil
		}))
	}

	return p, nil
}

func (s *Service) toRangePrm(req *objectV2.GetRangeRequest, stream objectSvc.GetObjectRangeStream) (*getsvc.RangePrm, error) {
	body := req.GetBody()

	addrV2 := body.GetAddress()
	if addrV2 == nil {
		return nil, errors.New("missing object address")
	}

	var addr oid.Address

	err := addr.ReadFromV2(*addrV2)
	if err != nil {
		return nil, fmt.Errorf("invalid object address: %w", err)
	}

	meta := req.GetMetaHeader()

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangePrm)
	p.SetCommonParameters(commonPrm)

	streamWrapper := &streamObjectRangeWriter{stream}

	p.WithAddress(addr)
	p.WithRawFlag(body.GetRaw())
	p.SetChunkWriter(streamWrapper)
	p.SetRange(object.NewRangeFromV2(body.GetRange()))

	err = p.Validate()
	if err != nil {
		return nil, fmt.Errorf("request params validation: %w", err)
	}

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once
		var globalProgress int

		key, err := s.keyStorage.GetKey(nil)
		if err != nil {
			return nil, err
		}

		p.SetRequestForwarder(groupAddressRequestForwarder(func(ctx context.Context, addr network.Address, c client.MultiAddressClient, pubkey []byte) (*object.Object, error) {
			var err error

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: #1165 think how to set the other fields
				metaHdr.SetOrigin(meta)
				writeCurrentVersion(metaHdr)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})

			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.ObjectPayloadRangeData implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go

			// open stream
			var rangeStream protoobject.ObjectService_GetRangeClient
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			err = c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
				rangeStream, err = protoobject.NewObjectServiceClient(conn).GetRange(ctx, req.ToGRPCMessage().(*protoobject.GetRangeRequest))
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("could not create Get payload range stream: %w", err)
			}

			var localProgress int

			for {
				// receive message from server stream
				resp, err := rangeStream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					internalclient.ReportError(c, err)
					return nil, fmt.Errorf("reading the response failed: %w", err)
				}

				// verify response key
				if err = internal.VerifyResponseKeyV2(pubkey, resp); err != nil {
					return nil, err
				}

				// verify response structure
				resp2 := new(objectV2.GetRangeResponse)
				if err = resp2.FromGRPCMessage(resp); err != nil {
					panic(err) // can only fail on wrong type, here it's correct
				}
				if err := signature.VerifyServiceMessage(resp2); err != nil {
					return nil, fmt.Errorf("could not verify %T: %w", resp, err)
				}

				if err = checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
					return nil, err
				}

				switch v := resp.GetBody().GetRangePart().(type) {
				case nil:
					return nil, fmt.Errorf("unexpected range type %T", v)
				case *protoobject.GetRangeResponse_Body_Chunk:
					origChunk := v.GetChunk()

					chunk := chunkToSend(globalProgress, localProgress, origChunk)
					if len(chunk) == 0 {
						localProgress += len(origChunk)
						continue
					}

					if err = streamWrapper.WriteChunk(chunk); err != nil {
						return nil, fmt.Errorf("could not write object chunk in GetRange forwarder: %w", err)
					}

					localProgress += len(origChunk)
					globalProgress += len(chunk)
				case *protoobject.GetRangeResponse_Body_SplitInfo:
					if v == nil || v.SplitInfo == nil {
						return nil, errors.New("nil split info oneof field")
					}
					var si2 objectV2.SplitInfo
					if err := si2.FromGRPCMessage(v.SplitInfo); err != nil {
						panic(err) // can only fail on wrong type, here it's correct
					}
					si := object.NewSplitInfoFromV2(&si2)

					return nil, object.NewSplitInfoError(si)
				}
			}

			return nil, nil
		}))
	}

	return p, nil
}

func (s *Service) toHashRangePrm(req *objectV2.GetRangeHashRequest) (*getsvc.RangeHashPrm, error) {
	body := req.GetBody()

	addrV2 := body.GetAddress()
	if addrV2 == nil {
		return nil, errors.New("missing object address")
	}

	var addr oid.Address

	err := addr.ReadFromV2(*addrV2)
	if err != nil {
		return nil, fmt.Errorf("invalid object address: %w", err)
	}

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangeHashPrm)
	p.SetCommonParameters(commonPrm)

	p.WithAddress(addr)

	if tok := commonPrm.SessionToken(); tok != nil {
		signerKey, err := s.keyStorage.GetKey(&util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		})
		if err != nil && errors.As(err, new(apistatus.SessionTokenNotFound)) {
			commonPrm.ForgetTokens()
			signerKey, err = s.keyStorage.GetKey(nil)
		}

		if err != nil {
			return nil, fmt.Errorf("fetching session key: %w", err)
		}

		p.WithCachedSignerKey(signerKey)
	}

	rngsV2 := body.GetRanges()
	rngs := make([]object.Range, len(rngsV2))

	for i := range rngsV2 {
		rngs[i] = *object.NewRangeFromV2(&rngsV2[i])
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

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once
		var key *ecdsa.PrivateKey

		key, err = s.keyStorage.GetKey(nil)
		if err != nil {
			return nil, err
		}

		p.SetRangeHashRequestForwarder(groupAddressRequestForwarder(func(ctx context.Context, addr network.Address, c client.MultiAddressClient, pubkey []byte) ([][]byte, error) {
			meta := req.GetMetaHeader()

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: #1165 think how to set the other fields
				metaHdr.SetOrigin(meta)
				writeCurrentVersion(metaHdr)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})
			if err != nil {
				return nil, err
			}

			var resp *protoobject.GetRangeHashResponse
			err = c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
				resp, err = protoobject.NewObjectServiceClient(conn).GetRangeHash(ctx, req.ToGRPCMessage().(*protoobject.GetRangeHashRequest))
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("GetRangeHash rpc failure: %w", err)
			}

			// verify response key
			if err = internal.VerifyResponseKeyV2(pubkey, resp); err != nil {
				return nil, err
			}

			// verify response structure
			resp2 := new(objectV2.GetRangeHashResponse)
			if err = resp2.FromGRPCMessage(resp); err != nil {
				panic(err) // can only fail on wrong type, here it's correct
			}
			if err := signature.VerifyServiceMessage(resp2); err != nil {
				return nil, fmt.Errorf("could not verify %T: %w", resp, err)
			}

			if err = checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
				return nil, err
			}

			return resp.GetBody().GetHashList(), nil
		}))
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

func (s *Service) toHeadPrm(_ context.Context, req *objectV2.HeadRequest, resp *objectV2.HeadResponse) (*getsvc.HeadPrm, error) {
	body := req.GetBody()

	addrV2 := body.GetAddress()
	if addrV2 == nil {
		return nil, errors.New("missing object address")
	}

	var objAddr oid.Address

	err := objAddr.ReadFromV2(*addrV2)
	if err != nil {
		return nil, fmt.Errorf("invalid object address: %w", err)
	}

	meta := req.GetMetaHeader()

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(getsvc.HeadPrm)
	p.SetCommonParameters(commonPrm)

	p.WithAddress(objAddr)
	p.WithRawFlag(body.GetRaw())
	p.SetHeaderWriter(&headResponseWriter{
		mainOnly: body.GetMainOnly(),
		body:     resp.GetBody(),
	})

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once

		p.SetRequestForwarder(groupAddressRequestForwarder(func(ctx context.Context, addr network.Address, c client.MultiAddressClient, pubkey []byte) (*object.Object, error) {
			var err error

			key, err := s.keyStorage.GetKey(nil)
			if err != nil {
				return nil, err
			}

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: #1165 think how to set the other fields
				metaHdr.SetOrigin(meta)
				writeCurrentVersion(metaHdr)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})

			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.GetObjectHeader implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go

			// send Head request
			var headResp *protoobject.HeadResponse
			err = c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
				headResp, err = protoobject.NewObjectServiceClient(conn).Head(ctx, req.ToGRPCMessage().(*protoobject.HeadRequest))
				return err
			})
			if err != nil {
				return nil, fmt.Errorf("sending the request failed: %w", err)
			}

			// verify response key
			if err = internal.VerifyResponseKeyV2(pubkey, headResp); err != nil {
				return nil, err
			}

			// verify response structure
			resp2 := new(objectV2.HeadResponse)
			if err = resp2.FromGRPCMessage(headResp); err != nil {
				panic(err) // can only fail on wrong type, here it's correct
			}
			if err := signature.VerifyServiceMessage(resp2); err != nil {
				return nil, fmt.Errorf("response verification failed: %w", err)
			}

			if err = checkStatus(headResp.GetMetaHeader().GetStatus()); err != nil {
				return nil, err
			}

			var (
				hdr   *protoobject.Header
				idSig *protorefs.Signature
			)

			switch v := headResp.GetBody().GetHead().(type) {
			case nil:
				return nil, fmt.Errorf("unexpected header type %T", v)
			case *protoobject.HeadResponse_Body_ShortHeader:
				if !body.GetMainOnly() {
					return nil, fmt.Errorf("wrong header part type: expected %T, received %T",
						(*objectV2.ShortHeader)(nil), (*objectV2.HeaderWithSignature)(nil),
					)
				}

				if v == nil || v.ShortHeader == nil {
					return nil, errors.New("nil short header oneof field")
				}

				h := v.ShortHeader
				hdr = &protoobject.Header{
					Version:         h.Version,
					OwnerId:         h.OwnerId,
					CreationEpoch:   h.CreationEpoch,
					PayloadLength:   h.PayloadLength,
					PayloadHash:     h.PayloadHash,
					ObjectType:      h.ObjectType,
					HomomorphicHash: h.HomomorphicHash,
				}
			case *protoobject.HeadResponse_Body_Header:
				if body.GetMainOnly() {
					return nil, fmt.Errorf("wrong header part type: expected %T, received %T",
						(*objectV2.HeaderWithSignature)(nil), (*objectV2.ShortHeader)(nil),
					)
				}

				if v == nil || v.Header == nil {
					return nil, errors.New("nil header oneof field")
				}

				if v.Header.Header == nil {
					return nil, errors.New("missing header")
				}

				hdr = v.Header.Header
				idSig = v.Header.Signature

				if idSig == nil {
					// TODO(@cthulhu-rider): #1387 use "const" error
					return nil, errors.New("missing signature")
				}

				binID := objAddr.Object().Marshal()

				var sig2 refs.Signature
				if err := sig2.FromGRPCMessage(idSig); err != nil {
					panic(err) // can only fail on wrong type, here it's correct
				}
				var sig neofscrypto.Signature
				if err := sig.ReadFromV2(sig2); err != nil {
					return nil, fmt.Errorf("can't read signature: %w", err)
				}

				if !sig.Verify(binID) {
					return nil, errors.New("invalid object ID signature")
				}
			case *protoobject.HeadResponse_Body_SplitInfo:
				if v == nil || v.SplitInfo == nil {
					return nil, errors.New("nil split info oneof field")
				}
				var si2 objectV2.SplitInfo
				if err := si2.FromGRPCMessage(v.SplitInfo); err != nil {
					panic(err) // can only fail on wrong type, here it's correct
				}
				si := object.NewSplitInfoFromV2(&si2)

				return nil, object.NewSplitInfoError(si)
			}

			mObj := &protoobject.Object{
				Signature: idSig,
				Header:    hdr,
			}
			objv2 := new(objectV2.Object)
			if err := objv2.FromGRPCMessage(mObj); err != nil {
				panic(err) // can only fail on wrong type, here it's correct
			}

			obj := object.NewFromV2(objv2)
			obj.SetID(objAddr.Object())

			// convert the object
			return obj, nil
		}))
	}

	return p, nil
}

func splitInfoResponse(info *object.SplitInfo) *objectV2.GetResponse {
	resp := new(objectV2.GetResponse)

	body := new(objectV2.GetResponseBody)
	resp.SetBody(body)

	body.SetObjectPart(info.ToV2())

	return resp
}

func splitInfoRangeResponse(info *object.SplitInfo) *objectV2.GetRangeResponse {
	resp := new(objectV2.GetRangeResponse)

	body := new(objectV2.GetRangeResponseBody)
	resp.SetBody(body)

	body.SetRangePart(info.ToV2())

	return resp
}

func setSplitInfoHeadResponse(info *object.SplitInfo, resp *objectV2.HeadResponse) {
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

func groupAddressRequestForwarder[V any](f func(context.Context, network.Address, client.MultiAddressClient, []byte) (V, error)) func(context.Context, client.NodeInfo, client.MultiAddressClient) (V, error) {
	return func(ctx context.Context, info client.NodeInfo, c client.MultiAddressClient) (V, error) {
		var (
			firstErr error
			res      V

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

			res, err = f(ctx, addr, c, key)

			return
		})

		return res, firstErr
	}
}

func writeCurrentVersion(metaHdr *session.RequestMetaHeader) {
	versionV2 := new(refs.Version)

	apiVersion := versionSDK.Current()
	apiVersion.WriteToV2(versionV2)

	metaHdr.SetVersion(versionV2)
}

func checkStatus(st *protostatus.Status) error {
	stV2 := new(status.Status)
	if err := stV2.FromGRPCMessage(st); err != nil {
		panic(err) // can only fail on wrong type, here it's correct
	}
	if !status.IsSuccess(stV2.Code()) {
		return apistatus.ErrorFromV2(stV2)
	}

	return nil
}

func chunkToSend(global, local int, chunk []byte) []byte {
	if global == local {
		return chunk
	}

	if local+len(chunk) <= global {
		// chunk has already been sent
		return nil
	}

	return chunk[global-local:]
}
