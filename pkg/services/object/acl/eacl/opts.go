package eacl

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
)

type morphStorage struct {
	w *wrapper.Wrapper
}

type signedEACLTable eacl.Table

func (s *signedEACLTable) ReadSignedData(buf []byte) ([]byte, error) {
	return (*eacl.Table)(s).Marshal(buf)
}

func (s *signedEACLTable) SignedDataSize() int {
	// TODO: add eacl.Table.Size method
	return (*eacl.Table)(s).ToV2().StableSize()
}

func (s *morphStorage) GetEACL(cid *container.ID) (*eacl.Table, error) {
	table, sig, err := s.w.GetEACL(cid)
	if err != nil {
		return nil, err
	}

	if err := signature.VerifyDataWithSource(
		(*signedEACLTable)(table),
		func() ([]byte, []byte) {
			return sig.Key(), sig.Sign()
		},
	); err != nil {
		return nil, errors.Wrap(err, "incorrect signature")
	}

	return table, nil
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
