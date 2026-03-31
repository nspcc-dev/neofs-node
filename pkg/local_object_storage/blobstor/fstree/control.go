package fstree

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"go.uber.org/zap"
)

// currentVersion contains current FSTree config version.
const currentVersion = 3

var migrateFrom = map[int]func(*FSTree, *fsDescriptor, string) error{
	1: (*FSTree).migrateDescriptorFrom1Version,
	2: (*FSTree).migrateDescriptorFrom2Version,
}

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
	Subtype string `json:"subtype"`
}

func (t *FSTree) descriptorPath() string {
	return filepath.Join(t.RootPath, ".fstree.json")
}

func writeDescriptor(path string, d fsDescriptor) error {
	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("encode descriptor to JSON: %w", err)
	}

	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write descriptor tmp: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename descriptor tmp: %w", err)
	}
	return nil
}

// Init implements common.Storage.
func (t *FSTree) Init(id common.ID) error {
	err := util.MkdirAllX(t.RootPath, t.Permissions)
	if err != nil {
		return fmt.Errorf("mkdir all for %q: %w", t.RootPath, err)
	}

	if !id.IsZero() {
		t.shardID = id
		t.shardIDSet = true
	}

	err = t.checkConfig()
	if err != nil {
		return err
	}

	t.log = t.log.With(
		zap.String("substorage", t.subtype),
		zap.String("shard_id", t.shardID.String()),
	)

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
			return fmt.Errorf("descriptor %q is missing, can't open read-only storage", descPath)
		}
		if !t.shardIDSet {
			t.shardID, err = common.NewID()
			if err != nil {
				return fmt.Errorf("generate shard ID: %w", err)
			}
			t.shardIDSet = true
		}
		// create new descriptor
		d := fsDescriptor{
			Version: currentVersion,
			Depth:   t.Depth,
			ShardID: t.shardID.String(),
			Subtype: t.subtype,
		}
		return writeDescriptor(descPath, d)
	}
	var d fsDescriptor
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if err = dec.Decode(&d); err != nil {
		_ = f.Close()
		return fmt.Errorf("decode descriptor from JSON: %w", err)
	}
	_ = f.Close()

	if d.Version > currentVersion {
		return fmt.Errorf("unsupported layout version: %d (current version: %d)", d.Version, currentVersion)
	}
	for d.Version < currentVersion {
		migrate, ok := migrateFrom[d.Version]
		if !ok {
			return fmt.Errorf("unsupported layout version: %d (current version: %d)", d.Version, currentVersion)
		}
		if err := migrate(t, &d, descPath); err != nil {
			return err
		}
	}
	if t.subtypeSet {
		if d.Subtype != t.subtype {
			return fmt.Errorf("subtype mismatch: on-disk subtype=%s, configured subtype=%s", d.Subtype, t.subtype)
		}
	} else {
		t.subtype = d.Subtype
	}

	if t.depthSet {
		if d.Depth != t.Depth {
			return fmt.Errorf("layout mismatch: on-disk depth=%d, configured depth=%d", d.Depth, t.Depth)
		}
	} else {
		t.Depth = d.Depth
	}
	if t.shardIDSet {
		if d.ShardID != t.shardID.String() {
			return fmt.Errorf("shard ID mismatch: on-disk shard ID=%s, configured shard ID=%s", d.ShardID, t.shardID)
		}
	} else {
		if d.ShardID == "" {
			t.shardID = common.ID{}
			t.shardIDSet = false
			return nil
		}
		id, err := common.DecodeIDString(d.ShardID)
		if err != nil {
			return fmt.Errorf("invalid shard ID %q in descriptor: %w", d.ShardID, err)
		}
		t.shardID = id
		t.shardIDSet = !id.IsZero()
	}
	return nil
}

// migrateDescriptorFrom1Version migrates descriptor from version 1 to version 2.
// In version 1, ShardID was path-based and needs to be updated during migration.
func (t *FSTree) migrateDescriptorFrom1Version(d *fsDescriptor, descPath string) error {
	if !t.shardIDSet && d.ShardID != "" {
		id, err := common.DecodeIDString(d.ShardID)
		if err == nil {
			t.shardID = id
			t.shardIDSet = !id.IsZero()
		}
	}

	if !t.shardIDSet {
		id, err := common.NewID()
		if err != nil {
			return fmt.Errorf("generate shard ID during migration: %w", err)
		}
		t.shardID = id
		t.shardIDSet = true
	}

	d.Version = 2
	d.ShardID = t.shardID.String()
	if !t.readOnly {
		if err := writeDescriptor(descPath, *d); err != nil {
			return fmt.Errorf("write migrated descriptor: %w", err)
		}
	}
	return nil
}

// migrateDescriptorFrom2Version migrates descriptor from version 2 to version 3
// by pinning the storage subtype in the descriptor.
func (t *FSTree) migrateDescriptorFrom2Version(d *fsDescriptor, descPath string) error {
	if !t.subtypeSet {
		return errors.New("can't migrate FSTree descriptor from v2 to v3 without explicit subtype")
	}
	d.Version = currentVersion
	d.Subtype = t.subtype
	if !t.readOnly {
		if err := writeDescriptor(descPath, *d); err != nil {
			return fmt.Errorf("write migrated descriptor: %w", err)
		}
	}
	return nil
}
