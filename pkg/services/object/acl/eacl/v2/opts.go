package v2

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
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

func WithServiceResponse(v Response) Option {
	return func(c *cfg) {
		c.msg = &responseXHeaderSource{
			resp: v,
		}
	}
}
