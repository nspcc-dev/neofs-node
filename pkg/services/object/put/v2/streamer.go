package putsvc

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/session"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

type streamer struct {
	stream     *putsvc.Streamer
	keyStorage *util.KeyStorage
	saveChunks bool
	init       *object.PutRequest
	chunks     []*object.PutRequest
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
			s.init = req
		}
	case *object.PutObjectPartChunk:
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
	st := session.NewTokenFromV2(meta.GetSessionToken())

	metaHdr.SetTTL(meta.GetTTL() - 1)
	metaHdr.SetOrigin(meta)
	req.SetMetaHeader(metaHdr)

	key, err := s.keyStorage.GetKey(st)
	if err != nil {
		return err
	}
	return signature.SignServiceMessage(key, req)
}

func (s *streamer) CloseAndRecv() (*object.PutResponse, error) {
	resp, err := s.stream.Close()
	if err != nil {
		return nil, fmt.Errorf("(%T) could not object put stream: %w", s, err)
	}

	return fromPutResponse(resp), nil
}

func (s *streamer) relayRequest(c client.Client) error {
	// open stream
	resp := new(object.PutResponse)

	stream, err := rpc.PutObject(c.Raw(), resp)
	if err != nil {
		return fmt.Errorf("stream opening failed: %w", err)
	}

	// send init part
	err = stream.Write(s.init)
	if err != nil {
		return fmt.Errorf("sending the initial message to stream failed: %w", err)
	}

	for i := range s.chunks {
		if err := stream.Write(s.chunks[i]); err != nil {
			return fmt.Errorf("sending the chunk %d failed: %w", i, err)
		}
	}

	// close object stream and receive response from remote node
	err = stream.Close()
	if err != nil {
		return fmt.Errorf("closing the stream failed: %w", err)
	}

	// verify response structure
	if err := signature.VerifyServiceMessage(resp); err != nil {
		return fmt.Errorf("response verification failed: %w", err)
	}
	return nil
}
