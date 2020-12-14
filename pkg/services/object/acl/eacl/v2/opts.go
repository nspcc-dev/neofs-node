package v2

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
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
		c.msg = &requestXHeaderSource{
			req: v,
		}
	}
}

func WithServiceResponse(v Response, addr *refs.Address) Option {
	return func(c *cfg) {
		c.msg = &responseXHeaderSource{
			resp: v,
			addr: addr,
		}
	}
}
