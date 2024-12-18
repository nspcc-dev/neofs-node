package searchsvc

import (
	"errors"
	"fmt"
	"io"
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
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"google.golang.org/grpc"
)

func (s *Service) toPrm(req *objectV2.SearchRequest, stream objectSvc.SearchStream) (*searchsvc.Prm, error) {
	body := req.GetBody()

	cnrV2 := body.GetContainerID()
	if cnrV2 == nil {
		return nil, errors.New("missing container ID")
	}

	var id cid.ID

	err := id.ReadFromV2(*cnrV2)
	if err != nil {
		return nil, fmt.Errorf("invalid container ID: %w", err)
	}

	meta := req.GetMetaHeader()

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(searchsvc.Prm)
	p.SetCommonParameters(commonPrm)

	p.SetWriter(&streamWriter{
		stream: stream,
	})

	if !commonPrm.LocalOnly() {
		var onceResign sync.Once

		key, err := s.keyStorage.GetKey(nil)
		if err != nil {
			return nil, err
		}

		p.SetRequestForwarder(groupAddressRequestForwarder(func(addr network.Address, c client.MultiAddressClient, pubkey []byte) ([]oid.ID, error) {
			var err error

			// once compose and resign forwarding request
			onceResign.Do(func() {
				// compose meta header of the local server
				metaHdr := new(session.RequestMetaHeader)
				metaHdr.SetTTL(meta.GetTTL() - 1)
				// TODO: #1165 think how to set the other fields
				metaHdr.SetOrigin(meta)

				req.SetMetaHeader(metaHdr)

				err = signature.SignServiceMessage(key, req)
			})

			if err != nil {
				return nil, err
			}

			var searchStream protoobject.ObjectService_SearchClient
			err = c.RawForAddress(addr, func(conn *grpc.ClientConn) error {
				// FIXME: context should be cancelled on return from upper func
				searchStream, err = protoobject.NewObjectServiceClient(conn).Search(stream.Context(), req.ToGRPCMessage().(*protoobject.SearchRequest))
				return err
			})
			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.SearchObjects implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go
			var searchResult []oid.ID

			for {
				// receive message from server stream
				resp, err := searchStream.Recv()
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
				resp2 := new(objectV2.SearchResponse)
				if err = resp2.FromGRPCMessage(resp); err != nil {
					panic(err) // can only fail on wrong type, here it's correct
				}
				if err := signature.VerifyServiceMessage(resp2); err != nil {
					return nil, fmt.Errorf("could not verify %T: %w", resp, err)
				}

				if err := checkStatus(resp.GetMetaHeader().GetStatus()); err != nil {
					return nil, fmt.Errorf("remote node response: %w", err)
				}

				chunk := resp.GetBody().GetIdList()
				var id oid.ID

				for i := range chunk {
					var id2 refs.ObjectID
					if err = id2.FromGRPCMessage(chunk[i]); err != nil {
						panic(err) // can only fail on wrong type, here it's correct
					}
					err = id.ReadFromV2(id2)
					if err != nil {
						return nil, fmt.Errorf("invalid object ID: %w", err)
					}

					searchResult = append(searchResult, id)
				}
			}

			return searchResult, nil
		}))
	}

	p.WithContainerID(id)
	p.WithSearchFilters(object.NewSearchFiltersFromV2(body.GetFilters()))

	return p, nil
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

func groupAddressRequestForwarder(f func(network.Address, client.MultiAddressClient, []byte) ([]oid.ID, error)) searchsvc.RequestForwarder {
	return func(info client.NodeInfo, c client.MultiAddressClient) ([]oid.ID, error) {
		var (
			firstErr error
			res      []oid.ID

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
