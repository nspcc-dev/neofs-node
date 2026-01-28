package searchsvc

import (
	"context"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	sdkclient "github.com/nspcc-dev/neofs-sdk-go/client"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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

	var opts sdkclient.PrmObjectSearch
	if exec.prm.common.TTL() < 2 {
		opts.MarkLocal()
	}
	if st := exec.prm.common.SessionToken(); st != nil {
		opts.WithinSession(*st)
	}
	if bt := exec.prm.common.BearerToken(); bt != nil {
		opts.WithBearerToken(*bt)
	}
	opts.WithXHeaders(exec.prm.common.XHeaders()...)

	rdr, err := c.client.ObjectSearchInit(ctx, exec.containerID(), user.NewAutoIDSigner(*key), opts)
	if err != nil {
		return nil, fmt.Errorf("init object searching in client: %w", err)
	}

	var ids []oid.ID

	err = rdr.Iterate(func(id oid.ID) bool {
		ids = append(ids, id)
		return false
	})
	if err != nil {
		return nil, fmt.Errorf("search objects using NeoFS API: %w", err)
	}

	return ids, nil
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
