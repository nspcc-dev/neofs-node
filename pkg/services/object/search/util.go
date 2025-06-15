package searchsvc

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type uniqueIDWriter struct {
	mtx sync.Mutex

	written map[oid.ID]struct{}

	writer IDListWriter
}

type clientConstructorWrapper struct {
	constructor ClientConstructor
}

type clientWrapper struct {
	client client.MultiAddressClient
}

type storageEngineWrapper struct {
	storage *engine.StorageEngine
}

func newUniqueAddressWriter(w IDListWriter) IDListWriter {
	return &uniqueIDWriter{
		written: make(map[oid.ID]struct{}),
		writer:  w,
	}
}

func (w *uniqueIDWriter) WriteIDs(list []oid.ID) error {
	w.mtx.Lock()

	for i := 0; i < len(list); i++ { // don't use range, slice mutates in body
		if _, ok := w.written[list[i]]; !ok {
			// mark address as processed
			w.written[list[i]] = struct{}{}
			continue
		}

		// exclude processed address
		list = append(list[:i], list[i+1:]...)
		i--
	}

	w.mtx.Unlock()

	return w.writer.WriteIDs(list)
}

func (c *clientConstructorWrapper) get(info client.NodeInfo) (searchClient, error) {
	clt, err := c.constructor.Get(info)
	if err != nil {
		return nil, err
	}

	return &clientWrapper{
		client: clt,
	}, nil
}

func (c *clientWrapper) searchObjects(ctx context.Context, exec *execCtx) ([]oid.ID, error) {
	if exec.prm.forwarder != nil {
		return exec.prm.forwarder(c.client)
	}

	var sessionInfo *util.SessionInfo

	if tok := exec.prm.common.SessionToken(); tok != nil {
		sessionInfo = &util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		}
	}

	key, err := exec.svc.keyStore.GetKey(sessionInfo)
	if err != nil {
		return nil, err
	}

	var prm internalclient.SearchObjectsPrm

	prm.SetContext(ctx)
	prm.SetClient(c.client)
	prm.SetPrivateKey(key)
	prm.SetSessionToken(exec.prm.common.SessionToken())
	prm.SetBearerToken(exec.prm.common.BearerToken())
	prm.SetTTL(exec.prm.common.TTL())
	prm.SetXHeaders(exec.prm.common.XHeaders())
	prm.SetContainerID(exec.containerID())
	prm.SetFilters(exec.searchFilters())

	res, err := internalclient.SearchObjects(prm)
	if err != nil {
		return nil, err
	}

	return res.IDList(), nil
}

func (e *storageEngineWrapper) search(exec *execCtx) ([]oid.ID, error) {
	r, err := e.storage.Select(exec.containerID(), exec.searchFilters())
	if err != nil {
		return nil, err
	}

	return idsFromAddresses(r), nil
}

func idsFromAddresses(addrs []oid.Address) []oid.ID {
	ids := make([]oid.ID, len(addrs))

	for i := range addrs {
		ids[i] = addrs[i].Object()
	}

	return ids
}
