package searchsvc

import (
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
)

type uniqueIDWriter struct {
	mtx sync.Mutex

	written map[string]struct{}

	writer IDListWriter
}

type clientCacheWrapper struct {
	cache *cache.ClientCache
}

type clientWrapper struct {
	client *client.Client
}

type storageEngineWrapper engine.StorageEngine

type traverseGeneratorWrapper util.TraverserGenerator

type nmSrcWrapper struct {
	nmSrc netmap.Source
}

func newUniqueAddressWriter(w IDListWriter) IDListWriter {
	return &uniqueIDWriter{
		written: make(map[string]struct{}),
		writer:  w,
	}
}

func (w *uniqueIDWriter) WriteIDs(list []*objectSDK.ID) error {
	w.mtx.Lock()

	for i := 0; i < len(list); i++ { // don't use range, slice mutates in body
		s := list[i].String()
		// standard stringer is quite costly, it is better
		// to facilitate the calculation of the key

		if _, ok := w.written[s]; !ok {
			// mark address as processed
			w.written[s] = struct{}{}
			continue
		}

		// exclude processed address
		list = append(list[:i], list[i+1:]...)
		i--
	}

	w.mtx.Unlock()

	return w.writer.WriteIDs(list)
}

func (c *clientCacheWrapper) get(addr string) (searchClient, error) {
	clt, err := c.cache.Get(addr)

	return &clientWrapper{
		client: clt,
	}, err
}

func (c *clientWrapper) searchObjects(exec *execCtx) ([]*objectSDK.ID, error) {
	return c.client.SearchObject(exec.context(),
		exec.remotePrm(),
		exec.callOptions()...)
}

func (e *storageEngineWrapper) search(exec *execCtx) ([]*objectSDK.ID, error) {
	r, err := (*engine.StorageEngine)(e).Select(new(engine.SelectPrm).
		WithFilters(exec.searchFilters()).
		WithContainerID(exec.containerID()),
	)
	if err != nil {
		return nil, err
	}

	return idsFromAddresses(r.AddressList()), nil
}

func idsFromAddresses(addrs []*objectSDK.Address) []*objectSDK.ID {
	ids := make([]*objectSDK.ID, len(addrs))

	for i := range addrs {
		ids[i] = addrs[i].ObjectID()
	}

	return ids
}

func (e *traverseGeneratorWrapper) generateTraverser(cid *container.ID, epoch uint64) (*placement.Traverser, error) {
	a := objectSDK.NewAddress()
	a.SetContainerID(cid)

	return (*util.TraverserGenerator)(e).GenerateTraverser(a, epoch)
}

func (n *nmSrcWrapper) currentEpoch() (uint64, error) {
	return n.nmSrc.Epoch()
}
