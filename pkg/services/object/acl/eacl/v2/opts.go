package v2

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func WithContext(ctx context.Context) Option {
	return func(c *cfg) {
		c.ctx = ctx
	}
}

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

func WithHeaderSource(hs HeaderSource) Option {
	return func(c *cfg) {
		c.headerSource = hs
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
			req:  requestXHeaderSource{req},
		}
	}
}

// WithObjectHeader uses an already decoded and validated object header and
// the original request. Neither is modified by the header source.
func WithObjectHeader(hdr *object.Object, req Request) Option {
	return func(c *cfg) {
		c.msg = decodedHeader{obj: hdr, req: requestXHeaderSource{req}}
	}
}

// WithObjectHeaderBinary uses b as the object header. The caller must not modify b
// while the header source is in use.
func WithObjectHeaderBinary(b []byte) Option {
	return func(c *cfg) {
		c.msg = binaryHeader(b)
	}
}

func WithCID(v cid.ID) Option {
	return func(c *cfg) {
		c.cnr = v
	}
}

func WithOID(v oid.ID) Option {
	return func(c *cfg) {
		c.obj = v
	}
}
