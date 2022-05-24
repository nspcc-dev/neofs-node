package v2

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

func WithCID(v cidSDK.ID) Option {
	return func(c *cfg) {
		c.cid = v
	}
}

func WithOID(v *oidSDK.ID) Option {
	return func(c *cfg) {
		c.oid = v
	}
}
