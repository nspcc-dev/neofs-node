package validate

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestCheckForUnknownFields(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "with all right fields",
			config: `
fschain:
  dial_timeout: 1m
  reconnections_number: 5
  reconnections_delay: 5s
  endpoints:
      - wss://fschain1.fs.neo.org:30333/ws
      - wss://fschain2.fs.neo.org:30333/ws
  validators:
    - 0283120f4c8c1fc1d792af5063d2def9da5fddc90bc1384de7fcfdda33c3860170
  consensus:
    magic: 15405
    committee:
      - 02b3622bf4017bdfe317c58aed5f4c753f206b7db896046fa7d774bbc4bf7f8dc2
      - 02103a7f7dd016558597f7960d27c516a4394fd968b9e65155eb4b013e4040406e
      - 03d90c07df63e690ce77912e10ab51acc944b66860237b608c4f8f8309e71ee699
      - 02a7bc55fe8684e0119768d104ba30795bdcc86619e864add26156723ed185cd62
    storage:
      type: boltdb
      path: ./db/fschain.bolt
    time_per_block: 1s
    max_traceable_blocks: 11520
    seed_nodes:
      - node2
      - node3:20333
    hardforks:
      name: 1730000
    validators_history:
      0: 4
      4: 1
      12: 4
    rpc:
      listen:
        - localhost
        - localhost:30334
      tls:
        enabled: false
        listen:
          - localhost:30335
          - localhost:30336
        cert_file: serv.crt
        key_file: serv.key
    p2p:
      dial_timeout: 1m
      proto_tick_interval: 2s
      listen:
        - localhost
        - localhost:20334
      peers:
        min: 1
        max: 5
        attempts: 20
      ping:
        interval: 30s
        timeout: 90s
    set_roles_in_genesis: true
`,
			wantErr: false,
		},
		{
			name: "unknown fschain.consensus.timeout",
			config: `
fschain:
  consensus:
    p2p:
      ping:
        interval: 30s
    timeout: 90s
    set_roles_in_genesis: true
`,
			wantErr: true,
		},
		{
			name: "fschain.consensus.storage.type expected type string",
			config: `
fschain:
  consensus:
    storage:
      type:
        path: ./db/fschain.bolt
`,
			wantErr: true,
		},
		{
			name: "unknown field fschain.attr",
			config: `
fschain:
  dial_timeout: 1m
  reconnections_number: 5
  attr: 123
`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tt.config)))

			err := ValidateStruct(v)
			fmt.Println(err)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckForUnknownFields() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckForUnknownFieldsExample(t *testing.T) {
	const exampleConfigPrefix = "../../../../config/"

	path := filepath.Join(exampleConfigPrefix, "example/ir.yaml")
	v := viper.New()
	v.SetConfigFile(path)

	require.NoError(t, v.ReadInConfig())
	require.NoError(t, ValidateStruct(v))
}
