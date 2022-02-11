package v2

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
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

func WithServiceResponse(resp Response, req Request) Option {
	return func(c *cfg) {
		c.msg = &responseXHeaderSource{
			resp: resp,
			req:  req,
		}
	}
}

func WithAddress(v *addressSDK.Address) Option {
	return func(c *cfg) {
		c.addr = v
	}
}
