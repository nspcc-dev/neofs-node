package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_migrateConfigToFstree(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		wantErr   bool
		resConfig string
	}{
		{
			name: "right config 1",
			config: `
storage:
  shard:
    default:

      blobstor:
        - perm: 0644
        - perm: 0644
          depth: 5
    0:
      resync_metabase: true
      metabase:
        path: /home/endrey/neo/neofs-node/storage/metabase
        perm: 0777
      blobstor:
        - path: /home/endrey/neo/neofs-node/storage/path/peapod.db
          type: peapod
          perm: 0600
        - path: /home/endrey/neo/neofs-node/storage/path/fstree
          type: fstree
          perm: 0600
          depth: 4
      writecache:
        enabled: false
      gc:
        remover_batch_size: 100
        remover_sleep_interval: 1m
`,
			wantErr: false,
			resConfig: `storage:
    shard:
        0:
            blobstor:
                - depth: 4
                  path: /home/endrey/neo/neofs-node/storage/path/fstree
                  perm: 384
                  type: fstree
            gc:
                remover_batch_size: 100
                remover_sleep_interval: 1m
            metabase:
                path: /home/endrey/neo/neofs-node/storage/metabase
                perm: 511
            resync_metabase: true
            writecache:
                enabled: false
        default:
            blobstor:
                - depth: 5
                  perm: 420
`,
		},
		{
			name: "right config 2, w/o peapod in shard",
			config: `
storage:
  shard:
    default:

      blobstor:
        - perm: 0644

    0:
      resync_metabase: true
      metabase:
        path: /home/endrey/neo/neofs-node/storage/metabase
        perm: 0777
      blobstor:
        - path: /home/endrey/neo/neofs-node/storage/path/fstree
          type: fstree
          perm: 0600
          depth: 4
      writecache:
        enabled: false
      gc:
        remover_batch_size: 100
        remover_sleep_interval: 1m
`,
			wantErr: false,
			resConfig: `storage:
    shard:
        0:
            blobstor:
                - depth: 4
                  path: /home/endrey/neo/neofs-node/storage/path/fstree
                  perm: 384
                  type: fstree
            gc:
                remover_batch_size: 100
                remover_sleep_interval: 1m
            metabase:
                path: /home/endrey/neo/neofs-node/storage/metabase
                perm: 511
            resync_metabase: true
            writecache:
                enabled: false
        default:
            blobstor: []
`,
		},
		{
			name: "fstree not provided",
			config: `
storage:
  shard:
    default:

      blobstor:
        - perm: 0644
          depth: 5
    0:
      resync_metabase: true
      metabase:
        path: /home/endrey/neo/neofs-node/storage/metabase
        perm: 0777
      blobstor:
        - path: /home/endrey/neo/neofs-node/storage/path/peapod.db
          type: peapod
          perm: 0600
      writecache:
        enabled: false
      gc:
        remover_batch_size: 100
        remover_sleep_interval: 1m
`,
			wantErr:   true,
			resConfig: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			srcConfigFile := filepath.Join(dir, "config.yaml")

			err := os.WriteFile(srcConfigFile, []byte(tt.config), 0o600)
			require.NoError(t, err)

			dstPath := filepath.Join(dir, "config_fstree.yaml")
			if err := migrateConfigToFstree(dstPath, srcConfigFile); (err != nil) != tt.wantErr {
				t.Errorf("migrateConfigToFstree() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				fData, err := os.ReadFile(dstPath)
				require.NoError(t, err)
				require.Equal(t, tt.resConfig, string(fData))
			}
		})
	}
}
