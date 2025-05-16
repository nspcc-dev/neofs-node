package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
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
		{
			name: "unknown field morph",
			config: `
morph:
  dial_timeout: 1m
  reconnections_number: 5
`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			configFilePath := filepath.Join(tempDir, "config.yaml")

			err := os.WriteFile(configFilePath, []byte(tt.config), 0644)
			require.NoError(t, err)

			_, err = newConfig(configFilePath)
			fmt.Println(err)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckForUnknownFields() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckForUnknownFieldsExample(t *testing.T) {
	const exampleConfigPrefix = "../../config/"

	path := filepath.Join(exampleConfigPrefix, "example/ir.yaml")

	cfg, err := newConfig(path)
	require.NoError(t, err)

	validators, err := keys.NewPublicKeysFromStrings([]string{
		"0283120f4c8c1fc1d792af5063d2def9da5fddc90bc1384de7fcfdda33c3860170",
	})
	require.NoError(t, err)

	committee, err := keys.NewPublicKeysFromStrings([]string{
		"02b3622bf4017bdfe317c58aed5f4c753f206b7db896046fa7d774bbc4bf7f8dc2",
		"02103a7f7dd016558597f7960d27c516a4394fd968b9e65155eb4b013e4040406e",
		"03d90c07df63e690ce77912e10ab51acc944b66860237b608c4f8f8309e71ee699",
		"02a7bc55fe8684e0119768d104ba30795bdcc86619e864add26156723ed185cd62",
	})
	require.NoError(t, err)

	autorizedKeys, err := keys.NewPublicKeysFromStrings([]string{
		"035839e45d472a3b7769a2a1bd7d54c4ccd4943c3b40f547870e83a8fcbfb3ce11",
		"028f42cfcb74499d7b15b35d9bff260a1c8d27de4f446a627406a382d8961486d6",
	})
	require.NoError(t, err)

	cfg.UnsetAll()
	require.Equal(t, *cfg, config.Config{
		Logger: config.Logger{
			Level:     "info",
			Encoding:  "console",
			Timestamp: false,
		},
		Wallet: config.Wallet{
			Path:     "/path/to/wallet.json",
			Address:  "NUHtW3eM6a4mmFCgyyr4rj4wygsTKB88XX",
			Password: "secret",
		},
		WithoutMainnet: false,
		FSChain: config.Chain{
			BasicChain: config.BasicChain{
				DialTimeout:         time.Minute,
				ReconnectionsNumber: 5,
				ReconnectionsDelay:  5 * time.Second,
				Endpoints: []string{
					"wss://sidechain1.fs.neo.org:30333/ws",
					"wss://sidechain2.fs.neo.org:30333/ws",
				},
			},
			Validators: validators,
			Consensus: config.Consensus{
				Magic:     15405,
				Committee: committee,
				Storage: config.Storage{
					Type: "boltdb",
					Path: "./db/morph.bolt",
				},
				TimePerBlock:                time.Second,
				MaxTraceableBlocks:          11520,
				MaxValidUntilBlockIncrement: 3600,
				SeedNodes:                   []string{"node2", "node3:20333"},
				Hardforks: config.Hardforks{
					Name: map[string]uint32{
						"name": 1730000,
					},
				},
				ValidatorsHistory: config.ValidatorsHistory{
					Height: map[uint32]uint32{
						0:  4,
						12: 4,
						4:  1,
					},
				},
				RPC: config.RPC{
					Listen:              []string{"localhost", "localhost:30334"},
					MaxWebSocketClients: 100,
					SessionPoolSize:     100,
					MaxGasInvoke:        200,
					TLS: config.TLS{
						Enabled:  false,
						Listen:   []string{"localhost:30335", "localhost:30336"},
						CertFile: "serv.crt",
						KeyFile:  "serv.key",
					},
				},
				P2P: config.P2P{
					DialTimeout:       time.Minute,
					ProtoTickInterval: 2 * time.Second,
					Listen:            []string{"localhost", "[fe80::55aa]:20333:7111", "localhost:20334"},
					Peers: config.Peers{
						Min:      1,
						Max:      5,
						Attempts: 20,
					},
					Ping: config.Ping{
						Interval: 30 * time.Second,
						Timeout:  90 * time.Second,
					},
				},
				SetRolesInGenesis:               true,
				KeepOnlyLatestState:             true,
				RemoveUntraceableBlocks:         false,
				P2PNotaryRequestPayloadPoolSize: 100,
			}},
		FSChainAutodeploy: true,
		NNS: config.NNS{
			SystemEmail: "usr@domain.io",
		},
		Mainnet: config.BasicChain{
			DialTimeout:         time.Minute,
			ReconnectionsNumber: 5,
			ReconnectionsDelay:  5 * time.Second,
			Endpoints: []string{
				"wss://mainchain1.fs.neo.org:30333/ws",
				"wss://mainchain.fs.neo.org:30333/ws",
			},
		},
		Control: config.Control{
			AuthorizedKeys: autorizedKeys,
			GRPC: config.GRPC{
				Endpoint: "localhost:8090",
			},
		},
		Governance: config.Governance{
			Disable: false,
		},
		Node: config.Node{
			PersistentState: config.PersistentState{
				Path: ".neofs-ir-state",
			},
		},
		Fee: config.Fee{
			MainChain: 50000000,
		},
		Timers: config.Timers{
			StopEstimation:        config.BasicTimer{Mul: 1, Div: 4},
			CollectBasicIncome:    config.BasicTimer{Mul: 1, Div: 2},
			DistributeBasicIncome: config.BasicTimer{Mul: 3, Div: 4},
		},
		Emit: config.Emit{
			Storage: config.EmitStorage{
				Amount: 800000000,
			},
			Mint: config.Mint{
				Value:     20000000,
				CacheSize: 1000,
				Threshold: 1,
			},
			Gas: config.Gas{
				BalanceThreshold: 100000000000,
			},
		},
		Workers: config.Workers{
			Alphabet:   10,
			Balance:    10,
			Container:  10,
			NeoFS:      10,
			Netmap:     10,
			Reputation: 10,
		},
		Audit: config.Audit{
			Timeout: config.Timeout{
				Get:       5 * time.Second,
				Head:      5 * time.Second,
				RangeHash: 5 * time.Second,
				Search:    10 * time.Second,
			},
			Task: config.Task{
				ExecPoolSize:  10,
				QueueCapacity: 100,
			},
			PDP: config.PDP{
				PairsPoolSize:    10,
				MaxSleepInterval: 5 * time.Second,
			},
			POR: config.POR{
				PoolSize: 10,
			},
		},
		Indexer: config.Indexer{
			CacheTimeout: 15 * time.Second,
		},
		Contracts: config.Contracts{
			NeoFS:      "ee3dee6d05dc79c24a5b8f6985e10d68b7cacc62",
			Processing: "597f5894867113a41e192801709c02497f611de8",
		},
		Pprof: config.BasicService{
			Enabled:         true,
			Address:         "localhost:6060",
			ShutdownTimeout: 30 * time.Second,
		},
		Prometheus: config.BasicService{
			Enabled:         true,
			Address:         "localhost:9090",
			ShutdownTimeout: 30 * time.Second,
		},
		Settlement: config.Settlement{
			BasicIncomeRate: 100,
			AuditFee:        100,
		},
		Experimental: config.Experimental{
			ChainMetaData: false,
		},
		Validator: config.Validator{
			Enabled: true,
			URL:     "http://localhost:8080/verify",
		},
	})
}
