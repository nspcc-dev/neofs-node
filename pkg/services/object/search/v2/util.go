package searchsvc

import (
	"errors"
	"fmt"
	"io"
	"sync"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	rpcclient "github.com/nspcc-dev/neofs-api-go/rpc/client"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

func (s *Service) toPrm(req *objectV2.SearchRequest, stream objectSvc.SearchStream) (*searchsvc.Prm, error) {
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

		p.SetRequestForwarder(groupAddressRequestForwarder(func(addr network.Address, c client.Client, pubkey []byte) ([]*objectSDK.ID, error) {
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

			stream, err := rpc.SearchObjects(c.RawForAddress(addr), req, rpcclient.WithContext(stream.Context()))
			if err != nil {
				return nil, err
			}

			// code below is copy-pasted from c.SearchObjects implementation,
			// perhaps it is worth highlighting the utility function in neofs-api-go
			var (
				searchResult []*objectSDK.ID
				resp         = new(objectV2.SearchResponse)
			)

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

				chunk := resp.GetBody().GetIDList()
				for i := range chunk {
					searchResult = append(searchResult, objectSDK.NewIDFromV2(chunk[i]))
				}
			}

			return searchResult, nil
		}))
	}

	body := req.GetBody()
	p.WithContainerID(cid.NewFromV2(body.GetContainerID()))
	p.WithSearchFilters(objectSDK.NewSearchFiltersFromV2(body.GetFilters()))

	return p, nil
}

func groupAddressRequestForwarder(f func(network.Address, client.Client, []byte) ([]*objectSDK.ID, error)) searchsvc.RequestForwarder {
	return func(info client.NodeInfo, c client.Client) ([]*objectSDK.ID, error) {
		var (
			firstErr error
			res      []*objectSDK.ID

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
