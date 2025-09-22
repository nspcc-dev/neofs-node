package fstree

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/util"
)

// currentVersion contains current FSTree config version.
const currentVersion = 1

// Open implements common.Storage.
func (t *FSTree) Open(ro bool) error {
	t.readOnly = ro
	return nil
}

// fsDescriptor is stored under the FSTree root and pins layout/config.
type fsDescriptor struct {
	Version int    `json:"version"`
	Depth   uint64 `json:"depth"`
	ShardID string `json:"shard_id"`
}

func (t *FSTree) descriptorPath() string {
	return filepath.Join(t.RootPath, ".fstree.json")
}

// Init implements common.Storage.
func (t *FSTree) Init() error {
	err := util.MkdirAllX(t.RootPath, t.Permissions)
	if err != nil {
		return fmt.Errorf("mkdir all for %q: %w", t.RootPath, err)
	}

	err = t.checkConfig()
	if err != nil {
		return err
	}

	if !t.readOnly {
		var w = newSpecificWriter(t)
		if w != nil {
			t.writer = w
		}
	}
	return nil
}

// Close implements common.Storage.
func (t *FSTree) Close() error {
	return t.writer.finalize()
}

func (t *FSTree) checkConfig() error {
	descPath := t.descriptorPath()
	f, err := os.Open(descPath)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("read descriptor %q: %w", descPath, err)
		}
		if t.readOnly {
			return nil
		}
		// create new descriptor
		d := fsDescriptor{
			Version: currentVersion,
			Depth:   t.Depth,
			ShardID: t.shardID,
		}
		data, err := json.Marshal(d)
		if err != nil {
			return fmt.Errorf("encode descriptor to JSON: %w", err)
		}
		tmp := descPath + ".tmp"
		if err = os.WriteFile(tmp, data, 0o600); err != nil {
			return fmt.Errorf("write descriptor tmp: %w", err)
		}
		if err = os.Rename(tmp, descPath); err != nil {
			return fmt.Errorf("rename descriptor tmp: %w", err)
		}
		return nil
	}
	var d fsDescriptor
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err = dec.Decode(&d); err != nil {
		return fmt.Errorf("decode descriptor from JSON: %w", err)
	}
	if d.Version != currentVersion {
		return fmt.Errorf("unsupported layout version: %d (current version: %d)", d.Version, currentVersion)
	}
	if d.Depth != t.Depth {
		return fmt.Errorf("layout mismatch: on-disk depth=%d, configured depth=%d", d.Depth, t.Depth)
	}
	if d.ShardID != t.shardID {
		return fmt.Errorf("shard ID mismatch: on-disk shard ID=%s, configured shard ID=%s", d.ShardID, t.shardID)
	}
	return nil
}
