package blobstorconfig

import (
	"strconv"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/storage"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
)

// Config is a wrapper over the config section
// which provides access to BlobStor configurations.
type Config config.Config

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Storages returns the value of storage subcomponents.
func (x *Config) Storages() []*storage.Config {
	var ss []*storage.Config
	for i := 0; ; i++ {
		typ := config.String(
			(*config.Config)(x),
			strconv.Itoa(i)+".type")
		switch typ {
		case "":
			return ss
		case fstree.Type, peapod.Type:
			sub := storage.From((*config.Config)(x).Sub(strconv.Itoa(i)))
			ss = append(ss, sub)
		default:
			panic("invalid type")
		}
	}
}
