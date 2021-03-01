package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
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
	log *zap.Logger
	js  nats.JetStreamContext
	nc  *nats.Conn

	m              *sync.RWMutex
	createdStreams map[string]struct{}
}

type opts struct {
	logger *zap.Logger
	nOpts  []nats.Option
}

type Option func(*opts)

var errConnIsClosed = errors.New("connection to the server is closed")

// Notify sends object address's string representation to the provided topic.
// Uses first 4 bytes of object ID as a message ID to support 'exactly once message'
// delivery.
//
// Returns error only if:
// 1. underlying connection was closed and has not established again;
// 2. NATS server could not respond that is has saved the message.
func (n *Writer) Notify(topic string, address *addressSDK.Address) error {
	if !n.nc.IsConnected() {
		return errConnIsClosed
	}

	// use first 4 byte of the encoded string as
	// message ID for the exactly-once delivery
	messageID := address.ObjectID().String()[:4]

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

	_, err := n.js.Publish(topic, []byte(address.String()), nats.MsgId(messageID))
	if err != nil {
		return err
	}

	return nil
}

// New creates and inits new Writer.
// Connection is closed when passed context is done.
//
// Returns error only if fails to open connection to a NATS server
// with provided configuration.
func New(ctx context.Context, endpoint string, oo ...Option) (*Writer, error) {
	opts := &opts{
		logger: zap.L(),
		nOpts:  make([]nats.Option, 0, len(oo)+3),
	}

	for _, o := range oo {
		o(opts)
	}

	opts.nOpts = append(opts.nOpts,
		nats.NoCallbacksAfterClientClose(), // do not call callbacks when it was planned writer stop
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			opts.logger.Error("nats: connection was lost", zap.Error(err))
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			opts.logger.Warn("nats: reconnected to the server")
		}),
	)

	nc, err := nats.Connect(endpoint, opts.nOpts...)
	if err != nil {
		return nil, fmt.Errorf("could not connect to server: %w", err)
	}

	// usage w/o options is error-free
	js, _ := nc.JetStream()

	go func() {
		<-ctx.Done()
		opts.logger.Info("nats: closing connection as the context is done")

		nc.Close()
	}()

	return &Writer{
		js:             js,
		nc:             nc,
		log:            opts.logger,
		m:              &sync.RWMutex{},
		createdStreams: make(map[string]struct{}),
	}, nil
}
