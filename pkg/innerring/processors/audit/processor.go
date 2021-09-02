package audit

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"time"

	SDKClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	wrapContainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	wrapNetmap "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// Indexer is a callback interface for inner ring global state.
	Indexer interface {
		InnerRingIndex() int
		InnerRingSize() int
	}

	// NeoFSClientCache is an interface for cache of neofs RPC clients
	NeoFSClientCache interface {
		Get(network.AddressGroup) (SDKClient.Client, error)
	}

	TaskManager interface {
		PushTask(*audit.Task) error

		// Must skip all tasks planned for execution and
		// return their number.
		Reset() int
	}

	// Processor of events related with data audit.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		irList        Indexer
		clientCache   NeoFSClientCache
		key           *ecdsa.PrivateKey
		searchTimeout time.Duration

		containerClient *wrapContainer.Wrapper
		netmapClient    *wrapNetmap.Wrapper

		taskManager       TaskManager
		reporter          audit.Reporter
		prevAuditCanceler context.CancelFunc
	}

	// Params of the processor constructor.
	Params struct {
		Log              *zap.Logger
		NetmapClient     *wrapNetmap.Wrapper
		ContainerClient  *wrapContainer.Wrapper
		IRList           Indexer
		ClientCache      NeoFSClientCache
		RPCSearchTimeout time.Duration
		TaskManager      TaskManager
		Reporter         audit.Reporter
		Key              *ecdsa.PrivateKey
	}
)

type epochAuditReporter struct {
	epoch uint64

	rep audit.Reporter
}

// ProcessorPoolSize limits pool size for audit Processor. Processor manages
// audit tasks and fills queue for next epoch. This process must not be interrupted
// by new audit epoch, so we limit pool size for processor to one.
const ProcessorPoolSize = 1

// New creates audit processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/audit: logger is not set")
	case p.IRList == nil:
		return nil, errors.New("ir/audit: global state is not set")
	case p.ClientCache == nil:
		return nil, errors.New("ir/audit: neofs RPC client cache is not set")
	case p.TaskManager == nil:
		return nil, errors.New("ir/audit: audit task manager is not set")
	case p.Reporter == nil:
		return nil, errors.New("ir/audit: audit result reporter is not set")
	case p.Key == nil:
		return nil, errors.New("ir/audit: signing key is not set")
	}

	pool, err := ants.NewPool(ProcessorPoolSize, ants.WithNonblocking(true))
	if err != nil {
		return nil, fmt.Errorf("ir/audit: can't create worker pool: %w", err)
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		containerClient:   p.ContainerClient,
		irList:            p.IRList,
		clientCache:       p.ClientCache,
		key:               p.Key,
		searchTimeout:     p.RPCSearchTimeout,
		netmapClient:      p.NetmapClient,
		taskManager:       p.TaskManager,
		reporter:          p.Reporter,
		prevAuditCanceler: func() {},
	}, nil
}

// ListenerNotificationParsers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotificationParsers() []event.NotificationParserInfo {
	return nil
}

// ListenerNotificationHandlers for the 'event.Listener' event producer.
func (ap *Processor) ListenerNotificationHandlers() []event.NotificationHandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (ap *Processor) TimersHandlers() []event.NotificationHandlerInfo {
	return nil
}

// StartAuditHandler for the internal event producer.
func (ap *Processor) StartAuditHandler() event.Handler {
	return ap.handleNewAuditRound
}

func (r *epochAuditReporter) WriteReport(rep *audit.Report) error {
	res := rep.Result()
	res.SetAuditEpoch(r.epoch)

	return r.rep.WriteReport(rep)
}
