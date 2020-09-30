package object

import (
	"bytes"
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/pkg/errors"
)

const (
	GRPCPayloadChunkSize = 1024 * 1024 * 3 // 4 MiB is a max limit, 3 MiB should be okay
	GRPCSearchAddrAmount = 1024 * 32       // 64 bytes per addr, in total about 2 MiB
)

var (
	errChunking = errors.New("can't split message to stream chunks")
)

type (
	TransportSplitter struct {
		next object.Service

		chunkSize  uint64
		addrAmount uint64
	}

	getStreamBasicChecker struct {
		next      object.GetObjectStreamer
		buf       *bytes.Buffer
		resp      *object.GetResponse
		chunkSize int
	}

	searchStreamBasicChecker struct {
		next       object.SearchObjectStreamer
		resp       *object.SearchResponse
		list       []*refs.ObjectID
		addrAmount uint64
	}

	rangeStreamBasicChecker struct {
		next      object.GetRangeObjectStreamer
		buf       *bytes.Buffer
		resp      *object.GetRangeResponse
		chunkSize int
	}
)

func NewTransportSplitter(size, amount uint64, next object.Service) *TransportSplitter {
	return &TransportSplitter{
		next:       next,
		chunkSize:  size,
		addrAmount: amount,
	}
}

func (c TransportSplitter) Get(ctx context.Context, request *object.GetRequest) (object.GetObjectStreamer, error) {
	stream, err := c.next.Get(ctx, request)

	return &getStreamBasicChecker{
		next:      stream,
		chunkSize: int(c.chunkSize),
	}, err
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

func (c TransportSplitter) GetRange(ctx context.Context, request *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	stream, err := c.next.GetRange(ctx, request)

	return &rangeStreamBasicChecker{
		next:      stream,
		chunkSize: int(c.chunkSize),
	}, err
}

func (c TransportSplitter) GetRangeHash(ctx context.Context, request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	return c.next.GetRangeHash(ctx, request)
}

func (g *getStreamBasicChecker) Recv() (*object.GetResponse, error) {
	if g.resp == nil {
		resp, err := g.next.Recv()
		if err != nil {
			return resp, err
		}

		if part, ok := resp.GetBody().GetObjectPart().(*object.GetObjectPartChunk); !ok {
			return resp, err
		} else {
			g.resp = resp
			g.buf = bytes.NewBuffer(part.GetChunk())
		}
	}

	chunkBody := new(object.GetObjectPartChunk)
	chunkBody.SetChunk(g.buf.Next(g.chunkSize))

	body := new(object.GetResponseBody)
	body.SetObjectPart(chunkBody)

	resp := new(object.GetResponse)
	resp.SetVerificationHeader(g.resp.GetVerificationHeader())
	resp.SetMetaHeader(g.resp.GetMetaHeader())
	resp.SetBody(body)

	if g.buf.Len() == 0 {
		g.buf = nil
		g.resp = nil
	}

	return resp, nil
}

func (r *rangeStreamBasicChecker) Recv() (*object.GetRangeResponse, error) {
	if r.resp == nil {
		resp, err := r.next.Recv()
		if err != nil {
			return resp, err
		}

		r.resp = resp
		r.buf = bytes.NewBuffer(resp.GetBody().GetChunk())
	}

	body := new(object.GetRangeResponseBody)
	body.SetChunk(r.buf.Next(r.chunkSize))

	resp := new(object.GetRangeResponse)
	resp.SetVerificationHeader(r.resp.GetVerificationHeader())
	resp.SetMetaHeader(r.resp.GetMetaHeader())
	resp.SetBody(body)

	if r.buf.Len() == 0 {
		r.buf = nil
		r.resp = nil
	}

	return resp, nil
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
