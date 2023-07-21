package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Writer is a NATS object notification writer.
// It handles NATS JetStream connections and allows
// sending string representation of the address to
// the NATS server.
//
// For correct operation must be created via New function.
// new(Writer) or Writer{} construction leads to undefined
// behaviour and is not safe.
type Writer struct {
	js nats.JetStreamContext
	nc *nats.Conn

	m              *sync.RWMutex
	createdStreams map[string]struct{}
	opts
}

type opts struct {
	log   *zap.Logger
	nOpts []nats.Option
}

type Option func(*opts)

var errConnIsClosed = errors.New("connection to the server is closed")

// Notify sends object address's string representation to the provided topic.
// Uses first 4 bytes of object ID as a message ID to support 'exactly once'
// message delivery.
//
// Returns error only if:
// 1. underlying connection was closed and has not been established again;
// 2. NATS server could not respond that it has saved the message.
func (n *Writer) Notify(topic string, address oid.Address) error {
	if !n.nc.IsConnected() {
		return errConnIsClosed
	}

	// use first 4 byte of the encoded string as
	// message ID for the 'exactly once' delivery
	messageID := address.Object().EncodeToString()[:4]

	// check if the stream was previously created
	n.m.RLock()
	_, created := n.createdStreams[topic]
	n.m.RUnlock()

	if !created {
		_, err := n.js.AddStream(&nats.StreamConfig{
			Name: topic,
		})
		if err != nil {
			return fmt.Errorf("could not add stream: %w", err)
		}

		n.m.Lock()
		n.createdStreams[topic] = struct{}{}
		n.m.Unlock()
	}

	_, err := n.js.Publish(topic, []byte(address.EncodeToString()), nats.MsgId(messageID))
	if err != nil {
		return err
	}

	return nil
}

// New creates new Writer.
func New(oo ...Option) *Writer {
	w := &Writer{
		m:              &sync.RWMutex{},
		createdStreams: make(map[string]struct{}),
		opts: opts{
			log:   zap.L(),
			nOpts: make([]nats.Option, 0, len(oo)+3),
		},
	}

	for _, o := range oo {
		o(&w.opts)
	}

	w.opts.nOpts = append(w.opts.nOpts,
		nats.NoCallbacksAfterClientClose(), // do not call callbacks when it was planned writer stop
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			w.log.Error("nats: connection was lost", zap.Error(err))
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			w.log.Warn("nats: reconnected to the server")
		}),
	)

	return w
}

// Connect tries to connect to a specified NATS endpoint.
//
// Connection is closed when passed context is done.
func (n *Writer) Connect(ctx context.Context, endpoint string) error {
	nc, err := nats.Connect(endpoint, n.opts.nOpts...)
	if err != nil {
		return fmt.Errorf("could not connect to server: %w", err)
	}

	n.nc = nc

	// usage w/o options is error-free
	n.js, _ = nc.JetStream()

	go func() {
		<-ctx.Done()
		n.opts.log.Info("nats: closing connection as the context is done")

		nc.Close()
	}()

	return nil
}
