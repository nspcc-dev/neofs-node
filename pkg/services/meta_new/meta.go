package meta

import (
	"context"
	"crypto/elliptic"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-node/pkg/services/sidechain"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const (
	// notificationBuffSize is a nesessary buffer for neo-go's client proper
	// notification work; it is required to always read notifications without
	// any blocking or making additional RPC.
	notificationBuffSize = 10000
)

func newNotifier(metaSvc *Meta) *objectNotifier {
	return &objectNotifier{
		metaSvc: metaSvc,
		subs:    make(map[oid.Address]objSubInfo),
	}
}

type objSubInfo struct {
	ch                       chan<- struct{}
	timeSubscriptionStarted  time.Time
	blockSubscriptionStarted uint32
}

type objectNotifier struct {
	metaSvc *Meta

	m    sync.Mutex
	subs map[oid.Address]objSubInfo
}

func (on *objectNotifier) subscribe(addr oid.Address, ch chan<- struct{}) {
	subTime := time.Now()
	subBlock := on.metaSvc.chainHeigh.Load()

	on.m.Lock()
	defer on.m.Unlock()

	on.subs[addr] = objSubInfo{
		ch:                       ch,
		timeSubscriptionStarted:  subTime,
		blockSubscriptionStarted: subBlock,
	}
}

func (on *objectNotifier) unsubscribe(addr oid.Address) {
	on.m.Lock()
	defer on.m.Unlock()

	delete(on.subs, addr)
}

func (on *objectNotifier) notifyReceived(addr oid.Address) {
	var (
		timeTook   time.Duration
		blocksTook uint32
		currHeight = on.metaSvc.chainHeigh.Load()
	)

	on.m.Lock()

	sub, ok := on.subs[addr]
	if ok {
		close(sub.ch)
		delete(on.subs, addr)

		timeTook = time.Since(sub.timeSubscriptionStarted)
		blocksTook = currHeight - sub.blockSubscriptionStarted
	}

	on.m.Unlock()

	if ok {
		on.metaSvc.l.Info("DEBUG: object notification handled", zap.Stringer("addr", addr), zap.Duration("timeTook", timeTook), zap.Uint32("blocksTook", blocksTook))
		on.metaSvc.metrics.objAcceptTime.Observe(timeTook.Seconds())
		on.metaSvc.metrics.objAcceptBlocks.Observe(float64(blocksTook))
	}
}

// Meta handles object meta information received from FS chain and object
// storages. Chain information is stored in Merkle-Patricia Tries. Full objects
// index is built and stored as a simple KV storage.
type Meta struct {
	l *zap.Logger

	metrics metrics

	chainHeigh  atomic.Uint32
	ch          *sidechain.SideChain
	magicNumber uint32
	bCh         chan *block.Header
	evsCh       chan *state.ContainedNotificationEvent

	notifier *objectNotifier
}

const blockBuffSize = 10000

// Parameters groups arguments for [New] call.
type Parameters struct {
	Logger *zap.Logger
	Chain  *sidechain.SideChain
}

func validatePrm(p Parameters) error {
	if p.Logger == nil {
		return errors.New("missing logger")
	}
	if p.Chain == nil {
		return errors.New("missing sidechain")
	}

	return nil
}

// New makes [Meta].
func New(p Parameters) (*Meta, error) {
	err := validatePrm(p)
	if err != nil {
		return nil, err
	}

	m := &Meta{
		l:     p.Logger,
		ch:    p.Chain,
		bCh:   make(chan *block.Header, blockBuffSize),
		evsCh: make(chan *state.ContainedNotificationEvent, notificationBuffSize),
	}
	notifier := newNotifier(m)
	m.notifier = notifier

	m.addMetrics()

	return m, nil
}

func (m *Meta) MagicNumber() uint32 {
	return m.magicNumber
}

// TODO
func (m *Meta) Height() uint32 {
	return m.ch.Height()
}

func (m *Meta) SubmitObjectPut(tx *transaction.Transaction, signatures [][]neofscrypto.Signature) error {
	for i := range signatures {
		slices.SortFunc(signatures[i], func(a, b neofscrypto.Signature) int {
			k1, err := keys.NewPublicKeyFromBytes(a.PublicKeyBytes(), elliptic.P256())
			if err != nil {
				panic(err)
			}
			k2, err := keys.NewPublicKeyFromBytes(b.PublicKeyBytes(), elliptic.P256())
			if err != nil {
				panic(err)
			}
			return k1.Cmp(k2)
		})
	}

	var (
		invokBuff = io.NewBufBinWriter()
		writer    = invokBuff.BinWriter
	)
	for i := len(signatures) - 1; i >= 0; i-- {
		vectorLen := len(signatures[i])
		for j := vectorLen - 1; j >= 0; j-- {
			emit.Bytes(writer, signatures[i][j].Value())
		}
		emit.Int(writer, int64(vectorLen))
		emit.Opcodes(writer, opcode.PACK)
	}

	if invokBuff.Err != nil {
		panic(invokBuff.Err)
	}

	tx.Scripts[0].InvocationScript = invokBuff.Bytes()

	return m.ch.AddTx(tx)
}

// Run starts notification handling. Must be called only on instances created
// with [New]. Blocked until context is done.
func (m *Meta) Run(ctx context.Context) error {
	m.magicNumber = m.ch.Magic()

	var wg sync.WaitGroup
	wg.Add(2)

	go m.blockHandler(ctx, m.bCh, &wg)
	go m.notificationHandler(ctx, m.evsCh, &wg)

	m.ch.SubscribeForBlocks(m.bCh)
	m.ch.SubscribeForNotifications(m.evsCh)

	wg.Wait()

	return nil
}
