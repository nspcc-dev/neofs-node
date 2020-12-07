package object

import (
	"bytes"
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/pkg/errors"
)

var (
	errChunking = errors.New("can't split message to stream chunks")
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

	searchStreamBasicChecker struct {
		next       object.SearchObjectStreamer
		resp       *object.SearchResponse
		list       []*refs.ObjectID
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

func (c TransportSplitter) Search(ctx context.Context, request *object.SearchRequest) (object.SearchObjectStreamer, error) {
	stream, err := c.next.Search(ctx, request)

	return &searchStreamBasicChecker{
		next:       stream,
		addrAmount: c.addrAmount,
	}, err
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

func (s *searchStreamBasicChecker) Recv() (*object.SearchResponse, error) {
	if s.resp == nil {
		resp, err := s.next.Recv()
		if err != nil {
			return resp, err
		}

		s.resp = resp
		s.list = s.resp.GetBody().GetIDList()
	}

	chunk := s.list[:min(int(s.addrAmount), len(s.list))]
	s.list = s.list[len(chunk):]

	body := new(object.SearchResponseBody)
	body.SetIDList(chunk)

	resp := new(object.SearchResponse)
	resp.SetVerificationHeader(s.resp.GetVerificationHeader())
	resp.SetMetaHeader(s.resp.GetMetaHeader())
	resp.SetBody(body)

	if len(s.list) == 0 {
		s.list = nil
		s.resp = nil
	}

	return resp, nil
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
