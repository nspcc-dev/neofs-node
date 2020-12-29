package object

import (
	"bytes"
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type (
	TransportSplitter struct {
		next ServiceServer

		chunkSize  uint64
		addrAmount uint64
	}

	getStreamMsgSizeCtrl struct {
		util.ServerStream

		stream GetObjectStream

		chunkSize int
	}

	searchStreamMsgSizeCtrl struct {
		util.ServerStream

		stream SearchStream

		addrAmount uint64
	}

	rangeStreamMsgSizeCtrl struct {
		util.ServerStream

		stream GetObjectRangeStream

		chunkSize int
	}
)

func (s *getStreamMsgSizeCtrl) Send(resp *object.GetResponse) error {
	body := resp.GetBody()

	part := body.GetObjectPart()

	chunkPart, ok := part.(*object.GetObjectPartChunk)
	if !ok {
		return s.stream.Send(resp)
	}

	var newResp *object.GetResponse

	for buf := bytes.NewBuffer(chunkPart.GetChunk()); buf.Len() > 0; {
		if newResp == nil {
			newResp = new(object.GetResponse)
			newResp.SetBody(body)
		}

		chunkPart.SetChunk(buf.Next(s.chunkSize))
		newResp.SetMetaHeader(resp.GetMetaHeader())
		newResp.SetVerificationHeader(resp.GetVerificationHeader())

		if err := s.stream.Send(newResp); err != nil {
			return err
		}
	}

	return nil
}

func NewTransportSplitter(size, amount uint64, next ServiceServer) *TransportSplitter {
	return &TransportSplitter{
		next:       next,
		chunkSize:  size,
		addrAmount: amount,
	}
}

func (c *TransportSplitter) Get(req *object.GetRequest, stream GetObjectStream) error {
	return c.next.Get(req, &getStreamMsgSizeCtrl{
		ServerStream: stream,
		stream:       stream,
		chunkSize:    int(c.chunkSize),
	})
}

func (c TransportSplitter) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	return c.next.Put(ctx)
}

func (c TransportSplitter) Head(ctx context.Context, request *object.HeadRequest) (*object.HeadResponse, error) {
	return c.next.Head(ctx, request)
}

func (c TransportSplitter) Search(req *object.SearchRequest, stream SearchStream) error {
	return c.next.Search(req, &searchStreamMsgSizeCtrl{
		ServerStream: stream,
		stream:       stream,
		addrAmount:   c.addrAmount,
	})
}

func (c TransportSplitter) Delete(ctx context.Context, request *object.DeleteRequest) (*object.DeleteResponse, error) {
	return c.next.Delete(ctx, request)
}

func (s *rangeStreamMsgSizeCtrl) Send(resp *object.GetRangeResponse) error {
	body := resp.GetBody()

	chunkPart, ok := body.GetRangePart().(*object.GetRangePartChunk)
	if !ok {
		return s.stream.Send(resp)
	}

	var newResp *object.GetRangeResponse

	for buf := bytes.NewBuffer(chunkPart.GetChunk()); buf.Len() > 0; {
		if newResp == nil {
			newResp = new(object.GetRangeResponse)
			newResp.SetBody(body)
		}

		chunkPart.SetChunk(buf.Next(s.chunkSize))
		body.SetRangePart(chunkPart)
		newResp.SetMetaHeader(resp.GetMetaHeader())
		newResp.SetVerificationHeader(resp.GetVerificationHeader())

		if err := s.stream.Send(newResp); err != nil {
			return err
		}
	}

	return nil
}

func (c TransportSplitter) GetRange(req *object.GetRangeRequest, stream GetObjectRangeStream) error {
	return c.next.GetRange(req, &rangeStreamMsgSizeCtrl{
		ServerStream: stream,
		stream:       stream,
		chunkSize:    int(c.chunkSize),
	})
}

func (c TransportSplitter) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	return c.next.GetRangeHash(ctx, request)
}

func (s *searchStreamMsgSizeCtrl) Send(resp *object.SearchResponse) error {
	body := resp.GetBody()
	ids := body.GetIDList()

	var newResp *object.SearchResponse

	for ln := uint64(len(ids)); len(ids) > 0; {
		if newResp == nil {
			newResp = new(object.SearchResponse)
			newResp.SetBody(body)
		}

		cut := s.addrAmount
		if cut > ln {
			cut = ln
		}

		body.SetIDList(ids[:cut])
		newResp.SetMetaHeader(resp.GetMetaHeader())
		newResp.SetVerificationHeader(resp.GetVerificationHeader())

		if err := s.stream.Send(newResp); err != nil {
			return err
		}

		ids = ids[cut:]
	}

	return nil
}
