package v2

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func WithObjectStorage(v ObjectStorage) Option {
	return func(c *cfg) {
		c.storage = v
	}
}

func WithLocalObjectStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.storage = &localStorage{
			ls: v,
		}
	}
}

func WithServiceRequest(v Request) Option {
	return func(c *cfg) {
		c.msg = requestXHeaderSource{
			req: v,
		}
	}
}

func WithServiceResponse(resp Response, req Request) Option {
	return func(c *cfg) {
		c.msg = responseXHeaderSource{
			resp: resp,
			req:  req,
		}
	}
}

func WithCID(v cid.ID) Option {
	return func(c *cfg) {
		c.cnr = v
	}
}

func WithOID(v *oid.ID) Option {
	return func(c *cfg) {
		c.obj = v
	}
}
