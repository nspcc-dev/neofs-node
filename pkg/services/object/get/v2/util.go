package getsvc

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"sync"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	protoobject "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/status"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"google.golang.org/grpc"
)

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

func toHashResponse(typ refs.ChecksumType, res *getsvc.RangeHashRes) *objectV2.GetRangeHashResponse {
	resp := new(objectV2.GetRangeHashResponse)

	body := new(objectV2.GetRangeHashResponseBody)
	resp.SetBody(body)

	body.SetType(typ)
	body.SetHashList(res.Hashes())

	return resp
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
