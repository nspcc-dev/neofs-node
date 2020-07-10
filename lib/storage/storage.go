package storage

import (
	"io"

	"github.com/nspcc-dev/neofs-node/lib/buckets"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type (
	store struct {
		blob core.Bucket

		meta core.Bucket

		spaceMetrics core.Bucket
	}

	sizer interface {
		Size() int64
	}

	// Params for create Core.Storage component
	Params struct {
		Buckets []core.BucketType
		Viper   *viper.Viper
		Logger  *zap.Logger
	}
)

// New creates Core.Storage component.
func New(p Params) (core.Storage, error) {
	var (
		err error
		bs  = make(map[core.BucketType]core.Bucket)
	)

	for _, name := range p.Buckets {
		if bs[name], err = buckets.NewBucket(name, p.Logger, p.Viper); err != nil {
			return nil, err
		}
	}

	return &store{
		blob: bs[core.BlobStore],

		meta: bs[core.MetaStore],

		spaceMetrics: bs[core.SpaceMetricsStore],
	}, nil
}

// GetBucket returns available bucket by type or an error.
func (s *store) GetBucket(name core.BucketType) (core.Bucket, error) {
	switch name {
	case core.BlobStore:
		if s.blob == nil {
			return nil, errors.Errorf("bucket(`%s`) not initialized", core.BlobStore)
		}

		return s.blob, nil
	case core.MetaStore:
		if s.meta == nil {
			return nil, errors.Errorf("bucket(`%s`) not initialized", core.MetaStore)
		}

		return s.meta, nil
	case core.SpaceMetricsStore:
		if s.spaceMetrics == nil {
			return nil, errors.Errorf("bucket(`%s`) not initialized", core.SpaceMetricsStore)
		}

		return s.spaceMetrics, nil
	default:
		return nil, errors.Errorf("bucket for type `%s` not implemented", name)
	}
}

// Size of all buckets.
func (s *store) Size() int64 {
	var (
		all    int64
		sizers = []sizer{
			s.blob,
			s.meta,
			s.spaceMetrics,
		}
	)

	for _, item := range sizers {
		if item == nil {
			continue
		}

		all += item.Size()
	}

	return all
}

// Close all buckets.
func (s *store) Close() error {
	var closers = []io.Closer{
		s.blob,
		s.meta,
	}

	for _, item := range closers {
		if item == nil {
			continue
		}

		if err := item.Close(); err != nil {
			return err
		}
	}

	return nil
}
