package searchsvc

import (
	"context"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	sdkclient "github.com/nspcc-dev/neofs-sdk-go/client"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
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

func (c *clientConstructorWrapper) get(ctx context.Context, info client.NodeInfo) (searchClient, error) {
	clt, err := c.constructor.Get(ctx, info)
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

	key, err := exec.svc.keyStore.GetKey(nil)
	if err != nil {
		return nil, err
	}
	if tokV2 := exec.prm.common.SessionTokenV2(); tokV2 != nil {
		// For V2 tokens, the key is stored as the subjects
		if keyForSession, err := exec.svc.keyStore.GetKeyBySubjects(tokV2.Subjects()); err == nil {
			key = keyForSession
		} else if exec.svc.nnsResolver != nil {
			nodeUser := user.NewFromECDSAPublicKey(key.PublicKey)
			ok, authErr := tokV2.AssertAuthority(nodeUser, exec.svc.nnsResolver)
			if authErr != nil {
				return nil, fmt.Errorf("assert authority for session v2 token: %w", authErr)
			}
			if !ok {
				return nil, fmt.Errorf("session v2 token authority assertion failed")
			}
			// node key is already in key
		} else {
			return nil, fmt.Errorf("get key for session v2 token: %w", err)
		}
	} else if tok := exec.prm.common.SessionToken(); tok != nil {
		authUser, err := tok.AuthUser()
		if err != nil {
			return nil, fmt.Errorf("could not get session auth user: %w", err)
		}
		key, err = exec.svc.keyStore.GetKey(&authUser)
		if err != nil {
			return nil, err
		}
	}

	var opts sdkclient.PrmObjectSearch
	if exec.prm.common.TTL() < 2 {
		opts.MarkLocal()
	}
	if stV2 := exec.prm.common.SessionTokenV2(); stV2 != nil {
		if stV2.AssertVerb(sessionv2.VerbObjectSearch, exec.containerID()) {
			opts.WithinSessionV2(*stV2)
		}
	} else if st := exec.prm.common.SessionToken(); st != nil {
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
