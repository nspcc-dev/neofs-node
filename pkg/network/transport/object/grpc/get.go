package object

import (
	"bytes"
	"io"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	getServerWriter struct {
		req *object.GetRequest

		srv object.Service_GetServer

		respPreparer responsePreparer
	}
)

const (
	maxGetPayloadSize = 3584 * 1024 // 3.5 MiB

	emSendObjectHead = "could not send object head"
)

var _ io.Writer = (*getServerWriter)(nil)

func (s *objectService) Get(req *object.GetRequest, server object.Service_GetServer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error(panicLogMsg,
				zap.Stringer("request", object.RequestGet),
				zap.Any("reason", r),
			)

			err = errServerPanic
		}

		err = s.statusCalculator.make(requestError{
			t: object.RequestGet,
			e: err,
		})
	}()

	var r interface{}

	if r, err = s.requestHandler.handleRequest(server.Context(), handleRequestParams{
		request:  req,
		executor: s,
	}); err != nil {
		return err
	}

	obj := r.(*objectData)

	var payload []byte
	payload, obj.Payload = obj.Payload, nil

	resp := makeGetHeaderResponse(obj.Object)
	if err = s.respPreparer.prepareResponse(server.Context(), req, resp); err != nil {
		return
	}

	if err = server.Send(resp); err != nil {
		return errors.Wrap(err, emSendObjectHead)
	}

	_, err = io.CopyBuffer(
		&getServerWriter{
			req:          req,
			srv:          server,
			respPreparer: s.getChunkPreparer,
		},
		io.MultiReader(bytes.NewReader(payload), obj.payload),
		make([]byte, maxGetPayloadSize))

	return err
}

func splitBytes(data []byte, maxSize int) (result [][]byte) {
	l := len(data)
	if l == 0 {
		return nil
	}

	for i := 0; i < l; i += maxSize {
		last := i + maxSize
		if last > l {
			last = l
		}

		result = append(result, data[i:last])
	}

	return
}

func (s *getServerWriter) Write(p []byte) (int, error) {
	resp := makeGetChunkResponse(p)
	if err := s.respPreparer.prepareResponse(s.srv.Context(), s.req, resp); err != nil {
		return 0, err
	}

	if err := s.srv.Send(resp); err != nil {
		return 0, err
	}

	return len(p), nil
}
