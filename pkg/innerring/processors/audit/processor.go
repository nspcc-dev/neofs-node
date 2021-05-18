package audit

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/util"
	SDKClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	wrapContainer "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	wrapNetmap "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
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

	// NeoFSClientCache is an interface for cache of neofs RPC clients
	NeoFSClientCache interface {
		Get(address string) (SDKClient.Client, error)
	}

	TaskManager interface {
		PushTask(*audit.Task) error

		// Must skip all tasks planned for execution and
		// return their number.
		Reset() int
	}

	// Processor of events related with data audit.
	Processor struct {
		log               *zap.Logger
		pool              *ants.Pool
		containerContract util.Uint160
		auditContract     util.Uint160
		morphClient       *client.Client
		irList            Indexer
		clientCache       NeoFSClientCache
		key               *ecdsa.PrivateKey
		searchTimeout     time.Duration

		containerClient *wrapContainer.Wrapper
		netmapClient    *wrapNetmap.Wrapper

		taskManager       TaskManager
		reporter          audit.Reporter
		prevAuditCanceler context.CancelFunc
	}

	// Params of the processor constructor.
	Params struct {
		Log               *zap.Logger
		NetmapContract    util.Uint160
		ContainerContract util.Uint160
		AuditContract     util.Uint160
		MorphClient       *client.Client
		IRList            Indexer
		FeeProvider       *config.FeeConfig
		ClientCache       NeoFSClientCache
		RPCSearchTimeout  time.Duration
		TaskManager       TaskManager
		Reporter          audit.Reporter
		Key               *ecdsa.PrivateKey
	}
)

type epochAuditReporter struct {
	epoch uint64

	rep audit.Reporter
}

// AuditProcessor manages audit tasks and fills queue for next epoch. This
// process must not be interrupted by new audit epoch, so we limit pool size
// for processor to one.
const ProcessorPoolSize = 1

// New creates audit processor instance.
func New(p *Params) (*Processor, error) {
	switch {
	case p.Log == nil:
		return nil, errors.New("ir/audit: logger is not set")
	case p.MorphClient == nil:
		return nil, errors.New("ir/audit: neo:morph client is not set")
	case p.IRList == nil:
		return nil, errors.New("ir/audit: global state is not set")
	case p.FeeProvider == nil:
		return nil, errors.New("ir/audit: fee provider is not set")
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

	// creating enhanced client for getting network map
	netmapClient, err := invoke.NewNetmapClient(p.MorphClient, p.NetmapContract, p.FeeProvider)
	if err != nil {
		return nil, err
	}

	// creating enhanced client for getting containers
	containerClient, err := invoke.NewContainerClient(p.MorphClient, p.ContainerContract, p.FeeProvider)
	if err != nil {
		return nil, err
	}

	return &Processor{
		log:               p.Log,
		pool:              pool,
		containerContract: p.ContainerContract,
		auditContract:     p.AuditContract,
		morphClient:       p.MorphClient,
		irList:            p.IRList,
		clientCache:       p.ClientCache,
		key:               p.Key,
		searchTimeout:     p.RPCSearchTimeout,
		containerClient:   containerClient,
		netmapClient:      netmapClient,
		taskManager:       p.TaskManager,
		reporter:          p.Reporter,
		prevAuditCanceler: func() {},
	}, nil
}

// ListenerParsers for the 'event.Listener' event producer.
func (ap *Processor) ListenerParsers() []event.ParserInfo {
	return nil
}

// ListenerHandlers for the 'event.Listener' event producer.
func (ap *Processor) ListenerHandlers() []event.HandlerInfo {
	return nil
}

// TimersHandlers for the 'Timers' event producer.
func (ap *Processor) TimersHandlers() []event.HandlerInfo {
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
