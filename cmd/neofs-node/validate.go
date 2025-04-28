package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"go.uber.org/zap/zapcore"
)

var (
	errEndpointNotSet = errors.New("empty/not set endpoint, see `grpc.endpoint` section")
	errTLSKeyNotSet   = errors.New("empty/not set TLS key file path, see `grpc.tls.key` section")
	errTLSCertNotSet  = errors.New("empty/not set TLS certificate file path, see `grpc.tls.certificate` section")
)

// validateConfig validates storage node configuration.
func validateConfig(c *config.Config) error {
	// logger configuration validation

	_, err := zapcore.ParseLevel(c.Logger.Level)
	if err != nil {
		return fmt.Errorf("invalid logger level: %w", err)
	}

	logEncoding := c.Logger.Encoding
	if logEncoding != "console" && logEncoding != "json" {
		return fmt.Errorf("invalid logger encoding: %s", logEncoding)
	}

	// chain configuration validation

	if len(c.FSChain.Endpoints) == 0 {
		return errors.New("no FS chain RPC endpoints, see `fschain.endpoints` section")
	}

	// grpc configuration validation

	if len(c.GRPC) == 0 {
		return errors.New("no gRPC endpoints, see `grpc` section")
	}
	for i := range c.GRPC {
		if c.GRPC[i].Endpoint == "" {
			return errEndpointNotSet
		}
		if c.GRPC[i].TLS.Enabled {
			if c.GRPC[i].TLS.Certificate == "" {
				return errTLSCertNotSet
			}
			if c.GRPC[i].TLS.Key == "" {
				return errTLSKeyNotSet
			}
		}
	}

	// shard configuration validation

	shardNum := 0
	paths := make(map[string]pathDescription)
	return engineconfig.IterateShards(&c.Storage, false, func(sc *shardconfig.ShardDetails) error {
		if !sc.Mode.IsValid() {
			return fmt.Errorf("unknown shard mode: %s (shard %d)", sc.Mode, shardNum)
		}
		if *sc.WriteCache.Enabled {
			err = addPath(paths, "writecache", shardNum, sc.WriteCache.Path)
			if err != nil {
				return err
			}
		}

		if err = addPath(paths, "metabase", shardNum, sc.Metabase.Path); err != nil {
			return err
		}

		blobstor := sc.Blobstor
		switch blobstor.Type {
		case fstree.Type:
		default:
			return fmt.Errorf("unexpected storage type: %s (shard %d)",
				blobstor.Type, shardNum)
		}
		if blobstor.Perm&0o600 != 0o600 {
			return fmt.Errorf("invalid permissions for blobstor component: %s, "+
				"expected at least rw- for the owner (shard %d)",
				blobstor.Perm, shardNum)
		}
		if blobstor.Path == "" {
			return fmt.Errorf("blobstor component path is empty (shard %d)", shardNum)
		}
		err = addPath(paths, "blobstor", shardNum, blobstor.Path)
		if err != nil {
			return err
		}

		shardNum++
		return nil
	})
}

type pathDescription struct {
	shard     int
	component string
}

func addPath(paths map[string]pathDescription, component string, shard int, path string) error {
	if path == "" {
		return fmt.Errorf("%s at shard %d has empty path", component, shard)
	}

	path = filepath.Clean(path)
	c, ok := paths[path]
	if ok {
		return fmt.Errorf("%s at shard %d and %s at shard %d have the same paths: %s",
			c.component, c.shard, component, shard, path)
	}

	paths[path] = pathDescription{shard: shard, component: component}
	return nil
}
