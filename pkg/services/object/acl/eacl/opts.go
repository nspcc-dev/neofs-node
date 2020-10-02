package eacl

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

type morphStorage struct {
	w *wrapper.Wrapper
}

func (s *morphStorage) GetEACL(cid *container.ID) (*eacl.Table, error) {
	table, _, err := s.w.GetEACL(cid)

	return table, err
}

func WithLogger(v *logger.Logger) Option {
	return func(c *cfg) {
		c.logger = v
	}
}

func WithEACLStorage(v Storage) Option {
	return func(c *cfg) {
		c.storage = v
	}
}

func WithMorphClient(v *wrapper.Wrapper) Option {
	return func(c *cfg) {
		c.storage = &morphStorage{
			w: v,
		}
	}
}
