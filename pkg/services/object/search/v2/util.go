package searchsvc

import (
	"errors"
	"fmt"
	"io"
	"sync"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc"
	rpcclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

			var searchStream *rpc.SearchResponseReader
			err = c.RawForAddress(addr, func(cli *rpcclient.Client) error {
				searchStream, err = rpc.SearchObjects(cli, req, rpcclient.WithContext(stream.Context()))
				return err
			})
			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.SearchObjects implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go
			var (
				searchResult []oid.ID
				resp         = new(objectV2.SearchResponse)
			)

			for {
				// receive message from server stream
				err := searchStream.Read(resp)
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

				chunk := resp.GetBody().GetIDList()
				var id oid.ID

				for i := range chunk {
					err = id.ReadFromV2(chunk[i])
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
