package putsvc

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
)

type streamer struct {
	stream     *putsvc.Streamer
	key        *ecdsa.PrivateKey
	saveChunks bool
	init       *object.PutRequest
	chunks     []*object.PutRequest

	*sizes // only for relay streams
}

type sizes struct {
	payloadSz uint64 // value from the header

	writtenPayload uint64 // sum size of already cached chunks
}

func (s *streamer) Send(req *object.PutRequest) (err error) {
	switch v := req.GetBody().GetObjectPart().(type) {
	case *object.PutObjectPartInit:
		var initPrm *putsvc.PutInitPrm

		initPrm, err = s.toInitPrm(v, req)
		if err != nil {
			return err
		}

		if err = s.stream.Init(initPrm); err != nil {
			err = fmt.Errorf("(%T) could not init object put stream: %w", s, err)
		}

		s.saveChunks = v.GetSignature() != nil
		if s.saveChunks {
			maxSz := s.stream.MaxObjectSize()

			s.sizes = &sizes{
				payloadSz: uint64(v.GetHeader().GetPayloadLength()),
			}

			// check payload size limit overflow
			if s.payloadSz > maxSz {
				return putsvc.ErrExceedingMaxSize
			}

			s.init = req
		}
	case *object.PutObjectPartChunk:
		if s.saveChunks {
			s.writtenPayload += uint64(len(v.GetChunk()))

			// check payload size overflow
			if s.writtenPayload > s.payloadSz {
				return putsvc.ErrWrongPayloadSize
			}
		}

		if err = s.stream.SendChunk(toChunkPrm(v)); err != nil {
			err = fmt.Errorf("(%T) could not send payload chunk: %w", s, err)
		}

		if s.saveChunks {
			s.chunks = append(s.chunks, req)
		}
	default:
		err = fmt.Errorf("(%T) invalid object put stream part type %T", s, v)
	}

	if err != nil || !s.saveChunks {
		return
	}

	metaHdr := new(sessionV2.RequestMetaHeader)
	meta := req.GetMetaHeader()

	metaHdr.SetTTL(meta.GetTTL() - 1)
	metaHdr.SetOrigin(meta)
	req.SetMetaHeader(metaHdr)

	return signature.SignServiceMessage(s.key, req)
}

func (s *streamer) CloseAndRecv() (*object.PutResponse, error) {
	if s.saveChunks {
		// check payload size correctness
		if s.writtenPayload != s.payloadSz {
			return nil, putsvc.ErrWrongPayloadSize
		}
	}

	resp, err := s.stream.Close()
	if err != nil {
		return nil, fmt.Errorf("(%T) could not object put stream: %w", s, err)
	}

	return fromPutResponse(resp), nil
}

func (s *streamer) relayRequest(info client.NodeInfo, c client.MultiAddressClient) error {
	// open stream
	resp := new(object.PutResponse)

	key := info.PublicKey()

	var firstErr error

	info.AddressGroup().IterateAddresses(func(addr network.Address) (stop bool) {
		var err error

		defer func() {
			stop = err == nil

			if stop || firstErr == nil {
				firstErr = err
			}

			// would be nice to log otherwise
		}()

		var stream *rpc.PutRequestWriter

		err = c.RawForAddress(addr, func(cli *rawclient.Client) error {
			stream, err = rpc.PutObject(cli, resp)
			return err
		})
		if err != nil {
			err = fmt.Errorf("stream opening failed: %w", err)
			return
		}

		// send init part
		err = stream.Write(s.init)
		if err != nil {
			internalclient.ReportError(c, err)
			err = fmt.Errorf("sending the initial message to stream failed: %w", err)
			return
		}

		for i := range s.chunks {
			if err = stream.Write(s.chunks[i]); err != nil {
				internalclient.ReportError(c, err)
				err = fmt.Errorf("sending the chunk %d failed: %w", i, err)
				return
			}
		}

		// close object stream and receive response from remote node
		err = stream.Close()
		if err != nil {
			err = fmt.Errorf("closing the stream failed: %w", err)
			return
		}

		// verify response key
		if err = internal.VerifyResponseKeyV2(key, resp); err != nil {
			return
		}

		// verify response structure
		err = signature.VerifyServiceMessage(resp)
		if err != nil {
			err = fmt.Errorf("response verification failed: %w", err)
		}

		return
	})

	return firstErr
}
