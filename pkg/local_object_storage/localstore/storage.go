package localstore

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Storage represents NeoFS local object storage.
type Storage struct {
	log *logger.Logger

	metaBase *meta.DB

	blobBucket bucket.Bucket
}

// Option is an option of Storage constructor.
type Option func(*cfg)

type cfg struct {
	logger *logger.Logger
}

func defaultCfg() *cfg {
	return &cfg{
		logger: zap.L(),
	}
}

// New is a local object storage constructor.
func New(blob bucket.Bucket, meta *meta.DB, opts ...Option) *Storage {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	return &Storage{
		log:        cfg.logger,
		metaBase:   meta,
		blobBucket: blob,
	}
}

// WithLogger returns Storage option of used logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		if l != nil {
			c.logger = l
		}
	}
}
