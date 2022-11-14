package blobstor

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// SubStorage represents single storage component with some storage policy.
type SubStorage struct {
	Storage common.Storage
	Policy  func(*objectSDK.Object, []byte) bool
}

// BlobStor represents NeoFS local BLOB storage.
type BlobStor struct {
	cfg

	modeMtx sync.RWMutex
	mode    mode.Mode
}

// Info contains information about blobstor.
type Info struct {
	SubStorages []SubStorageInfo
}

// SubStorageInfo contains information about blobstor storage component.
type SubStorageInfo struct {
	Type string
	Path string
}

// Option represents BlobStor's constructor option.
type Option func(*cfg)

type cfg struct {
	compression compression.Config
	log         *logger.Logger
	storage     []SubStorage
}

func initConfig(c *cfg) {
	c.log = &logger.Logger{Logger: zap.L()}
}

// New creates, initializes and returns new BlobStor instance.
func New(opts ...Option) *BlobStor {
	bs := new(BlobStor)
	initConfig(&bs.cfg)

	for i := range opts {
		opts[i](&bs.cfg)
	}

	for i := range bs.storage {
		bs.storage[i].Storage.SetCompressor(&bs.compression)
	}

	return bs
}

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (b *BlobStor) SetLogger(l *logger.Logger) {
	b.log = l
}

// WithStorages provides sub-blobstors.
func WithStorages(st []SubStorage) Option {
	return func(c *cfg) {
		c.storage = st
	}
}

// WithLogger returns option to specify BlobStor's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = &logger.Logger{Logger: l.With(zap.String("component", "BlobStor"))}
	}
}

// WithCompressObjects returns option to toggle
// compression of the stored objects.
//
// If true, Zstandard algorithm is used for data compression.
//
// If compressor (decompressor) creation failed,
// the uncompressed option will be used, and the error
// is recorded in the provided log.
func WithCompressObjects(comp bool) Option {
	return func(c *cfg) {
		c.compression.Enabled = comp
	}
}

// WithUncompressableContentTypes returns option to disable decompression
// for specific content types as seen by object.AttributeContentType attribute.
func WithUncompressableContentTypes(values []string) Option {
	return func(c *cfg) {
		c.compression.UncompressableContentTypes = values
	}
}

// SetReportErrorFunc allows to provide a function to be called on disk errors.
// This function MUST be called before Open.
func (b *BlobStor) SetReportErrorFunc(f func(string, error)) {
	for i := range b.storage {
		b.storage[i].Storage.SetReportErrorFunc(f)
	}
}
