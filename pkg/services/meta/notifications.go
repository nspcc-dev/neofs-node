package meta

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const (
	objPutEvName  = "ObjectPut"
	cnrDeleteName = "DeleteSuccess"
	cnrPutName    = "PutSuccess"
	newEpochName  = "NewEpoch"
)

// subscribeForBlocks reqauires [Meta.cliM] to be taken.
func (m *Meta) subscribeForBlocks(ch chan<- *block.Header) (string, error) {
	m.l.Debug("subscribe for blocks")
	return m.ws.ReceiveHeadersOfAddedBlocks(nil, ch)
}

func (m *Meta) unsubscribeFromBlocks() {
	var err error
	m.cliM.Lock()
	defer m.cliM.Unlock()

	err = m.ws.Unsubscribe(m.blockSubID)
	if err != nil {
		m.l.Warn("could not unsubscribe from blocks", zap.String("ID", m.blockSubID))
		return
	}

	m.blockSubID = ""

	m.l.Debug("successfully unsubscribed from blocks")
}

func (m *Meta) subscribeForMeta() error {
	cnrDeleteEv := cnrDeleteName
	_, err := m.ws.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &m.cnrH, Name: &cnrDeleteEv}, m.cnrDelEv)
	if err != nil {
		return fmt.Errorf("subscribe for container removal notifications: %w", err)
	}

	cnrPutEv := cnrPutName
	_, err = m.ws.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &m.cnrH, Name: &cnrPutEv}, m.cnrPutEv)
	if err != nil {
		return fmt.Errorf("subscribe for container addition notifications: %w", err)
	}

	epochEv := newEpochName
	_, err = m.ws.ReceiveExecutionNotifications(&neorpc.NotificationFilter{Contract: &m.netmapH, Name: &epochEv}, m.epochEv)
	if err != nil {
		return fmt.Errorf("subscribe for epoch notifications: %w", err)
	}

	return nil
}

func (m *Meta) listenNotifications(ctx context.Context) error {
	for {
		select {
		case h, ok := <-m.bCh:
			if !ok {
				err := m.reconnect(ctx)
				if err != nil {
					return err
				}

				continue
			}

			m.blockBuff <- h
		case aer, ok := <-m.cnrDelEv:
			if !ok {
				err := m.reconnect(ctx)
				if err != nil {
					return err
				}

				continue
			}

			l := m.l.With(zap.Stringer("notification container", aer.Container))

			ev, err := parseCnrNotification(aer)
			if err != nil {
				l.Error("invalid container notification received", zap.Error(err))
				continue
			}

			m.m.RLock()
			_, ok = m.storages[ev.cID]
			m.m.RUnlock()
			if !ok {
				l.Debug("skipping container notification", zap.Stringer("inactual container", ev.cID))
				continue
			}

			go func() {
				err = m.dropContainer(ev.cID)
				if err != nil {
					l.Error("deleting container failed", zap.Error(err))
					return
				}

				l.Debug("deleted container", zap.Stringer("cID", ev.cID))
			}()
		case aer, ok := <-m.cnrPutEv:
			if !ok {
				err := m.reconnect(ctx)
				if err != nil {
					return err
				}

				continue
			}

			l := m.l.With(zap.Stringer("notification container", aer.Container))

			ev, err := parseCnrNotification(aer)
			if err != nil {
				l.Error("invalid container notification received", zap.Error(err))
				continue
			}

			ok, err = m.cLister.IsMineWithMeta(ev.cID)
			if err != nil {
				l.Error("can't get container data", zap.Error(err))
				continue
			}
			if !ok {
				continue
			}

			err = m.addContainer(ev.cID)
			if err != nil {
				return fmt.Errorf("could not handle new %s container: %w", ev.cID, err)
			}

			l.Debug("added container storage", zap.Stringer("cID", ev.cID))
		case aer, ok := <-m.epochEv:
			if !ok {
				err := m.reconnect(ctx)
				if err != nil {
					return err
				}

				continue
			}

			l := m.l.With(zap.Stringer("notification container", aer.Container))

			epoch, err := parseEpochNotification(aer)
			if err != nil {
				l.Error("invalid new epoch notification received", zap.Error(err))
				continue
			}

			go func() {
				err = m.handleEpochNotification(epoch)
				if err != nil {
					l.Error("handling new epoch notification", zap.Int64("epoch", epoch), zap.Error(err))
					return
				}
			}()
		case <-ctx.Done():
			m.l.Info("stop listening meta notifications")
			return nil
		}
	}
}

func (m *Meta) reconnect(ctx context.Context) error {
	m.l.Warn("reconnecting to web socket client due to connection lost")

	m.cliM.Lock()
	defer m.cliM.Unlock()

	var err error
	m.ws, err = m.connect(ctx)
	if err != nil {
		return fmt.Errorf("reconnecting to web socket: %w", err)
	}

	m.m.RLock()
	if len(m.storages) > 0 {
		m.bCh = make(chan *block.Header)
		m.blockSubID, err = m.subscribeForBlocks(m.bCh)
		if err != nil {
			m.m.RUnlock()
			return fmt.Errorf("subscription for blocks: %w", err)
		}
	}
	m.m.RUnlock()

	m.cnrDelEv = make(chan *state.ContainedNotificationEvent)
	m.cnrPutEv = make(chan *state.ContainedNotificationEvent)
	m.epochEv = make(chan *state.ContainedNotificationEvent)

	err = m.subscribeForMeta()
	if err != nil {
		return fmt.Errorf("subscribe for meta notifications: %w", err)
	}

	return nil
}

func (m *Meta) connect(ctx context.Context) (*rpcclient.WSClient, error) {
	m.cfgM.RLock()
	endpoints := slices.Clone(m.endpoints)
	m.cfgM.RUnlock()

	var cli *rpcclient.WSClient
	var err error
outer:
	for {
		for _, e := range endpoints {
			cli, err = rpcclient.NewWS(ctx, e, rpcclient.WSOptions{
				Options: rpcclient.Options{
					DialTimeout: m.timeout,
				},
			})
			if err == nil {
				break outer
			}

			m.l.Warn("creating rpc client", zap.String("endpoint", e), zap.Error(err))
		}

		const reconnectionCooldown = time.Second * 5
		m.l.Error("FS chain reconnection failed", zap.Duration("cooldown time", reconnectionCooldown))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(reconnectionCooldown):
		}
	}

	err = cli.Init()
	if err != nil {
		return nil, fmt.Errorf("web socket client initializing: %w", err)
	}

	return cli, nil
}

const (
	// MPT key prefixes.
	oidIndex = iota
	sizeIndex
	firstPartIndex
	previousPartIndex
	deletedIndex
	lockedIndex
	typeIndex

	lastEnumIndex
)

const (
	// meta map keys from FS chain.
	cidKey          = "cid"
	oidKey          = "oid"
	sizeKey         = "size"
	validUntilKey   = "validUntil"
	networkMagicKey = "network"
	firstPartKey    = "firstPart"
	previousPartKey = "previousPart"
	deletedKey      = "deleted"
	lockedKey       = "locked"
	typeKey         = "type"
)

type objEvent struct {
	cID            cid.ID
	oID            oid.ID
	size           *big.Int
	network        *big.Int
	firstObject    []byte
	prevObject     []byte
	deletedObjects []byte
	lockedObjects  []byte
	typ            objectsdk.Type
}

func parseObjNotification(ev state.ContainedNotificationEvent) (objEvent, error) {
	const expectedNotificationArgs = 3
	var res objEvent

	arr, ok := ev.Item.Value().([]stackitem.Item)
	if !ok {
		return res, fmt.Errorf("unexpected notification stack item: %T", ev.Item.Value())
	}
	if len(arr) != expectedNotificationArgs {
		return res, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
	}

	cID, ok := arr[0].Value().([]byte)
	if !ok {
		return res, fmt.Errorf("unexpected container ID stack item: %T", arr[0].Value())
	}
	oID, ok := arr[1].Value().([]byte)
	if !ok {
		return res, fmt.Errorf("unexpected object ID stack item: %T", arr[1].Value())
	}
	meta, ok := arr[2].(*stackitem.Map)
	if !ok {
		return res, fmt.Errorf("unexpected meta stack item: %T", arr[2])
	}

	if len(cID) != cid.Size {
		return res, fmt.Errorf("unexpected container ID len: %d", len(cID))
	}
	if len(oID) != oid.Size {
		return res, fmt.Errorf("unexpected object ID len: %d", len(oID))
	}

	res.cID = cid.ID(cID)
	res.oID = oid.ID(oID)

	v := getFromMap(meta, sizeKey)
	if v == nil {
		return res, fmt.Errorf("missing '%s' key", sizeKey)
	}
	res.size, ok = v.Value().(*big.Int)
	if !ok {
		return res, fmt.Errorf("unexpected object size type: %T", v.Value())
	}

	v = getFromMap(meta, networkMagicKey)
	if v == nil {
		return res, fmt.Errorf("missing '%s' key", networkMagicKey)
	}
	res.network, ok = v.Value().(*big.Int)
	if !ok {
		return res, fmt.Errorf("unexpected network type: %T", v.Value())
	}

	v = getFromMap(meta, firstPartKey)
	if v != nil {
		res.firstObject, ok = v.Value().([]byte)
		if !ok {
			return res, fmt.Errorf("unexpected first part type: %T", v.Value())
		}
	}

	v = getFromMap(meta, previousPartKey)
	if v != nil {
		res.prevObject, ok = v.Value().([]byte)
		if !ok {
			return res, fmt.Errorf("unexpected previous part type: %T", v.Value())
		}
	}

	v = getFromMap(meta, typeKey)
	if v != nil {
		typ, ok := v.Value().(*big.Int)
		if !ok {
			return res, fmt.Errorf("unexpected object type field: %T", v.Value())
		}
		res.typ = objectsdk.Type(typ.Uint64())

		switch res.typ {
		case objectsdk.TypeTombstone:
			v = getFromMap(meta, deletedKey)
			if v == nil {
				return res, fmt.Errorf("missing '%s' key for %s object type", deletedKey, res.typ)
			}
			stackDeleted := v.Value().([]stackitem.Item)
			for i, d := range stackDeleted {
				rawDeleted, ok := d.Value().([]byte)
				if !ok {
					return res, fmt.Errorf("unexpected %d deleted object type: %T", i, d.Value())
				}
				res.deletedObjects = append(res.deletedObjects, rawDeleted...)
			}
		case objectsdk.TypeLock:
			v = getFromMap(meta, lockedKey)
			if v == nil {
				return res, fmt.Errorf("missing '%s' key for %s object type", lockedKey, res.typ)
			}
			stackLocked := v.Value().([]stackitem.Item)
			for i, d := range stackLocked {
				rawLocked, ok := d.Value().([]byte)
				if !ok {
					return res, fmt.Errorf("unexpected %d locked object type: %T", i, d.Value())
				}
				res.lockedObjects = append(res.deletedObjects, rawLocked...)
			}
		case objectsdk.TypeLink, objectsdk.TypeRegular:
		default:
			return res, fmt.Errorf("unknown '%s' object type", res.typ)
		}
	}

	return res, nil
}

func getFromMap(m *stackitem.Map, key string) stackitem.Item {
	i := m.Index(stackitem.Make(key))
	if i < 0 {
		return nil
	}

	return m.Value().([]stackitem.MapElement)[i].Value
}

func (m *Meta) handleObjectNotification(s *containerStorage, e objEvent) error {
	if magic := uint32(e.network.Uint64()); magic != m.magicNumber {
		return fmt.Errorf("wrong magic number %d, expected: %d", magic, m.magicNumber)
	}

	err := s.putObject(e)
	if err != nil {
		return err
	}

	return nil
}

type cnrEvent struct {
	cID cid.ID
}

func parseCnrNotification(ev *state.ContainedNotificationEvent) (cnrEvent, error) {
	var res cnrEvent

	arr, ok := ev.Item.Value().([]stackitem.Item)
	if !ok {
		return res, fmt.Errorf("unexpected notification stack item: %T", ev.Item.Value())
	}

	switch ev.Name {
	case cnrDeleteName:
		const expectedNotificationArgs = 1
		if len(arr) != expectedNotificationArgs {
			return res, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
		}
	case cnrPutName:
		const expectedNotificationArgs = 2
		if len(arr) != expectedNotificationArgs {
			return res, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
		}
	}

	cID, ok := arr[0].Value().([]byte)
	if !ok {
		return res, fmt.Errorf("unexpected container ID stack item: %T", arr[0].Value())
	}
	if len(cID) != cid.Size {
		return res, fmt.Errorf("unexpected container ID len: %d", len(cID))
	}

	return cnrEvent{cID: cid.ID(cID)}, nil
}

func (m *Meta) dropContainer(cID cid.ID) error {
	m.m.Lock()
	defer m.m.Unlock()

	st, ok := m.storages[cID]
	if !ok {
		return nil
	}

	err := st.drop()
	if err != nil {
		m.l.Warn("drop container %s: %w", zap.Stringer("cID", cID), zap.Error(err))
	}

	delete(m.storages, cID)

	if len(m.storages) == 0 {
		m.unsubscribeFromBlocks()
	}

	return nil
}

func (m *Meta) addContainer(cID cid.ID) error {
	var err error
	m.m.Lock()
	defer m.m.Unlock()

	if len(m.storages) == 0 {
		m.cliM.Lock()

		m.blockSubID, err = m.subscribeForBlocks(m.bCh)
		if err != nil {
			m.cliM.Unlock()
			return fmt.Errorf("blocks subscription: %w", err)
		}

		m.cliM.Unlock()
	}

	st, err := storageForContainer(m.rootPath, cID)
	if err != nil {
		return fmt.Errorf("open new storage for %s container: %w", cID, err)
	}
	m.storages[cID] = st

	return nil
}

func parseEpochNotification(ev *state.ContainedNotificationEvent) (int64, error) {
	const expectedNotificationArgs = 1

	arr, ok := ev.Item.Value().([]stackitem.Item)
	if !ok {
		return 0, fmt.Errorf("unexpected notification stack item: %T", ev.Item.Value())
	}
	if len(arr) != expectedNotificationArgs {
		return 0, fmt.Errorf("unexpected number of items on stack: %d, expected: %d", len(arr), expectedNotificationArgs)
	}

	epoch, ok := arr[0].Value().(*big.Int)
	if !ok {
		return 0, fmt.Errorf("unexpected epoch stack item: %T", arr[0].Value())
	}

	return epoch.Int64(), nil
}

func (m *Meta) handleEpochNotification(e int64) error {
	m.l.Debug("handling new epoch notification", zap.Int64("epoch", e))

	cnrsNetwork, err := m.cLister.List()
	if err != nil {
		return fmt.Errorf("list containers: %w", err)
	}

	m.m.Lock()
	defer m.m.Unlock()

	for cID, st := range m.storages {
		_, ok := cnrsNetwork[cID]
		if !ok {
			err = st.drop()
			if err != nil {
				m.l.Warn("drop inactual container", zap.Int64("epoch", e), zap.Stringer("cID", cID), zap.Error(err))
			}

			delete(m.storages, cID)
		}
	}
	for cID := range cnrsNetwork {
		if _, ok := m.storages[cID]; ok {
			continue
		}

		st, err := storageForContainer(m.rootPath, cID)
		if err != nil {
			return fmt.Errorf("create storage for container %s: %w", cID, err)
		}

		m.storages[cID] = st
	}

	m.cliM.Lock()
	defer m.cliM.Unlock()
	if len(m.storages) > 0 && m.blockSubID == "" {
		m.blockSubID, err = m.subscribeForBlocks(m.bCh)
		if err != nil {
			return fmt.Errorf("blocks subscription: %w", err)
		}
	} else if len(m.storages) == 0 && m.blockSubID != "" {
		m.unsubscribeFromBlocks()
	}

	m.l.Debug("handled new epoch successfully", zap.Int64("epoch", e))

	return nil
}
