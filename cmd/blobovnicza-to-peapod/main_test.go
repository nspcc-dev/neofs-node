package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigMigration(t *testing.T) {
	dir := t.TempDir()
	srcConfigFile := filepath.Join(dir, "config.yaml")

	const configYAML = `
storage:
  shard:
    0:
      blobstor:
      - path: /srv/neofs/data0/blobovnicza
        type: blobovnicza
      - path: /srv/neofs/data0/tree
        type: fstree
      metabase:
        path: /srv/neofs/meta/metabase0.db
      writecache:
        path: /srv/neofs/meta/writecache0
    1:
      blobstor:
      - path: /srv/neofs/data1/blobovnicza
        type: blobovnicza
      - path: /srv/neofs/data1/tree
        type: fstree
      metabase:
        path: /srv/neofs/meta/metabase1.db
      writecache:
        path: /srv/neofs/meta/writecache1
    default:
      blobstor:
      - depth: 1
        opened_cache_capacity: 50
        perm: '0644'
        size: 4gb
        width: 4
      - depth: 5
        perm: '0644'
      compress: true
      gc:
        remover_batch_size: 100
        remover_sleep_interval: 1m
      metabase:
        max_batch_delay: 20ms
        max_batch_size: 200
        perm: '0640'
      pilorama:
        max_batch_delay: 5ms
        max_batch_size: 100
      resync_metabase: false
      small_object_size: 100 kb
      writecache:
        capacity: 3gb
        enabled: true
        max_object_size: 128mb
        small_size: 32kb
        workers_number: 30
  shard_ro_error_threshold: 0
`

	err := os.WriteFile(srcConfigFile, []byte(configYAML), 0o600)
	require.NoError(t, err)

	err = migrateConfigToPeapod(filepath.Join(dir, "config_peapod.yaml"), srcConfigFile)
	require.NoError(t, err)
}
