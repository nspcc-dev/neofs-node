package audit

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/storagegroup"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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

	TaskManager interface {
		PushTask(*audit.Task) error

		// Must skip all tasks planned for execution and
		// return their number.
		Reset() int
	}

	// EpochSource is an interface that provides actual
	// epoch information.
	EpochSource interface {
		// EpochCounter must return current epoch number.
		EpochCounter() uint64
	}

	// Processor of events related to data audit.
	Processor struct {
		log           *zap.Logger
		pool          *ants.Pool
		irList        Indexer
		sgSrc         storagegroup.SGSource
		epochSrc      EpochSource
		searchTimeout time.Duration

		containerClient *cntClient.Client
		netmapClient    *nmClient.Client

		taskManager       TaskManager
		reporter          audit.Reporter
		prevAuditCanceler context.CancelFunc
	}

	// Params of the processor constructor.
	Params struct {
		Log              *zap.Logger
		NetmapClient     *nmClient.Client
		ContainerClient  *cntClient.Client
		IRList           Indexer
		SGSource         storagegroup.SGSource
		RPCSearchTimeout time.Duration
		TaskManager      TaskManager
		Reporter         audit.Reporter
		Key              *ecdsa.PrivateKey
		EpochSource      EpochSource
	}
)

type epochAuditReporter struct {
	epoch uint64

	rep audit.Reporter
}

// ProcessorPoolSize limits pool size for audit Processor. Processor manages
// audit tasks and fills queue for the next epoch. This process must not be interrupted
// by a new audit epoch, so we limit the pool size for the processor to one.
const ProcessorPoolSize = 1

// New creates audit processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/audit: logger is not set")
	case p.IRList == nil:
		return nil, errors.New("ir/audit: global state is not set")
	case p.SGSource == nil:
		return nil, errors.New("ir/audit: SG source is not set")
	case p.TaskManager == nil:
		return nil, errors.New("ir/audit: audit task manager is not set")
	case p.Reporter == nil:
		return nil, errors.New("ir/audit: audit result reporter is not set")
	case p.Key == nil:
		return nil, errors.New("ir/audit: signing key is not set")
	case p.EpochSource == nil:
		return nil, errors.New("ir/audit: epoch source is not set")
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
		sgSrc:             p.SGSource,
		epochSrc:          p.EpochSource,
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
	res.ForEpoch(r.epoch)

	return r.rep.WriteReport(rep)
}
