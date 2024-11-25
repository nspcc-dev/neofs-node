package validate

import "time"

type validConfig struct {
	Logger struct {
		Level    string `mapstructure:"level"`
		Encoding string `mapstructure:"encoding"`
	} `mapstructure:"logger"`

	Wallet struct {
		Path     string `mapstructure:"path"`
		Address  string `mapstructure:"address"`
		Password string `mapstructure:"password"`
	} `mapstructure:"wallet"`

	WithoutMainnet bool `mapstructure:"without_mainnet"`

	Morph struct {
		DialTimeout         time.Duration `mapstructure:"dial_timeout"`
		ReconnectionsNumber int           `mapstructure:"reconnections_number"`
		ReconnectionsDelay  time.Duration `mapstructure:"reconnections_delay"`
		Endpoints           []string      `mapstructure:"endpoints"`
		Validators          []string      `mapstructure:"validators"`
		Consensus           struct {
			Magic     uint32   `mapstructure:"magic"`
			Committee []string `mapstructure:"committee"`

			Storage struct {
				Type string `mapstructure:"type"`
				Path string `mapstructure:"path"`
			} `mapstructure:"storage"`

			TimePerBlock       time.Duration `mapstructure:"time_per_block"`
			MaxTraceableBlocks uint32        `mapstructure:"max_traceable_blocks"`
			SeedNodes          []string      `mapstructure:"seed_nodes"`

			Hardforks struct {
				Name map[string]uint32 `mapstructure:",remain" prefix:""`
			} `mapstructure:"hardforks"`

			ValidatorsHistory struct {
				Height map[string]int `mapstructure:",remain" prefix:""`
			} `mapstructure:"validators_history"`

			RPC struct {
				Listen []string `mapstructure:"listen"`
				TLS    struct {
					Enabled  bool     `mapstructure:"enabled"`
					Listen   []string `mapstructure:"listen"`
					CertFile string   `mapstructure:"cert_file"`
					KeyFile  string   `mapstructure:"key_file"`
				} `mapstructure:"tls"`
			} `mapstructure:"rpc"`

			P2P struct {
				DialTimeout       time.Duration `mapstructure:"dial_timeout"`
				ProtoTickInterval time.Duration `mapstructure:"proto_tick_interval"`
				Listen            []string      `mapstructure:"listen"`
				Peers             struct {
					Min      int `mapstructure:"min"`
					Max      int `mapstructure:"max"`
					Attempts int `mapstructure:"attempts"`
				} `mapstructure:"peers"`
				Ping struct {
					Interval time.Duration `mapstructure:"interval"`
					Timeout  time.Duration `mapstructure:"timeout"`
				} `mapstructure:"ping"`
			} `mapstructure:"p2p"`
			SetRolesInGenesis bool `mapstructure:"set_roles_in_genesis"`
		} `mapstructure:"consensus"`
	} `mapstructure:"morph"`

	FSChain struct {
		DialTimeout         time.Duration `mapstructure:"dial_timeout"`
		ReconnectionsNumber int           `mapstructure:"reconnections_number"`
		ReconnectionsDelay  time.Duration `mapstructure:"reconnections_delay"`
		Endpoints           []string      `mapstructure:"endpoints"`
		Validators          []string      `mapstructure:"validators"`
		Consensus           struct {
			Magic     uint32   `mapstructure:"magic"`
			Committee []string `mapstructure:"committee"`

			Storage struct {
				Type string `mapstructure:"type"`
				Path string `mapstructure:"path"`
			} `mapstructure:"storage"`

			TimePerBlock       time.Duration `mapstructure:"time_per_block"`
			MaxTraceableBlocks uint32        `mapstructure:"max_traceable_blocks"`
			SeedNodes          []string      `mapstructure:"seed_nodes"`

			Hardforks struct {
				Name map[string]uint32 `mapstructure:",remain" prefix:""`
			} `mapstructure:"hardforks"`

			ValidatorsHistory struct {
				Height map[string]int `mapstructure:",remain" prefix:""`
			} `mapstructure:"validators_history"`

			RPC struct {
				Listen []string `mapstructure:"listen"`
				TLS    struct {
					Enabled  bool     `mapstructure:"enabled"`
					Listen   []string `mapstructure:"listen"`
					CertFile string   `mapstructure:"cert_file"`
					KeyFile  string   `mapstructure:"key_file"`
				} `mapstructure:"tls"`
			} `mapstructure:"rpc"`

			P2P struct {
				DialTimeout       time.Duration `mapstructure:"dial_timeout"`
				ProtoTickInterval time.Duration `mapstructure:"proto_tick_interval"`
				Listen            []string      `mapstructure:"listen"`
				Peers             struct {
					Min      int `mapstructure:"min"`
					Max      int `mapstructure:"max"`
					Attempts int `mapstructure:"attempts"`
				} `mapstructure:"peers"`
				Ping struct {
					Interval time.Duration `mapstructure:"interval"`
					Timeout  time.Duration `mapstructure:"timeout"`
				} `mapstructure:"ping"`
			} `mapstructure:"p2p"`
			SetRolesInGenesis bool `mapstructure:"set_roles_in_genesis"`
		} `mapstructure:"consensus"`
	} `mapstructure:"fschain"`

	FSChainAutodeploy bool `mapstructure:"fschain_autodeploy"`

	NNS struct {
		SystemEmail string `mapstructure:"system_email"`
	} `mapstructure:"nns"`

	Mainnet struct {
		DialTimeout         time.Duration `mapstructure:"dial_timeout"`
		ReconnectionsNumber int           `mapstructure:"reconnections_number"`
		ReconnectionsDelay  time.Duration `mapstructure:"reconnections_delay"`
		Endpoints           []string      `mapstructure:"endpoints"`
	} `mapstructure:"mainnet"`

	Control struct {
		AuthorizedKeys []string `mapstructure:"authorized_keys"`
		GRPC           struct {
			Endpoint string `mapstructure:"endpoint"`
		} `mapstructure:"grpc"`
	} `mapstructure:"control"`

	Governance struct {
		Disable bool `mapstructure:"disable"`
	} `mapstructure:"governance"`

	Node struct {
		PersistentState struct {
			Path string `mapstructure:"path"`
		} `mapstructure:"persistent_state"`
	} `mapstructure:"node"`

	Fee struct {
		MainChain int64 `mapstructure:"main_chain"`
	} `mapstructure:"fee"`

	Timers struct {
		StopEstimation struct {
			Mul int `mapstructure:"mul"`
			Div int `mapstructure:"div"`
		} `mapstructure:"stop_estimation"`
		CollectBasicIncome struct {
			Mul int `mapstructure:"mul"`
			Div int `mapstructure:"div"`
		} `mapstructure:"collect_basic_income"`
		DistributeBasicIncome struct {
			Mul int `mapstructure:"mul"`
			Div int `mapstructure:"div"`
		} `mapstructure:"distribute_basic_income"`
	} `mapstructure:"timers"`

	Emit struct {
		Storage struct {
			Amount int64 `mapstructure:"amount"`
		} `mapstructure:"storage"`
		Mint struct {
			Value     int64 `mapstructure:"value"`
			CacheSize int   `mapstructure:"cache_size"`
			Threshold int   `mapstructure:"threshold"`
		} `mapstructure:"mint"`
		Gas struct {
			BalanceThreshold int64 `mapstructure:"balance_threshold"`
		} `mapstructure:"gas"`
	} `mapstructure:"emit"`

	Workers struct {
		Alphabet   int `mapstructure:"alphabet"`
		Balance    int `mapstructure:"balance"`
		Container  int `mapstructure:"container"`
		NeoFS      int `mapstructure:"neofs"`
		Netmap     int `mapstructure:"netmap"`
		Reputation int `mapstructure:"reputation"`
	} `mapstructure:"workers"`

	Audit struct {
		Timeout struct {
			Get       time.Duration `mapstructure:"get"`
			Head      time.Duration `mapstructure:"head"`
			RangeHash time.Duration `mapstructure:"rangehash"`
			Search    time.Duration `mapstructure:"search"`
		} `mapstructure:"timeout"`
		Task struct {
			ExecPoolSize  int `mapstructure:"exec_pool_size"`
			QueueCapacity int `mapstructure:"queue_capacity"`
		} `mapstructure:"task"`
		PDP struct {
			PairsPoolSize    int           `mapstructure:"pairs_pool_size"`
			MaxSleepInterval time.Duration `mapstructure:"max_sleep_interval"`
		} `mapstructure:"pdp"`
		POR struct {
			PoolSize int `mapstructure:"pool_size"`
		} `mapstructure:"por"`
	} `mapstructure:"audit"`

	Indexer struct {
		CacheTimeout time.Duration `mapstructure:"cache_timeout"`
	} `mapstructure:"indexer"`

	NetmapCleaner struct {
		Enabled   bool `mapstructure:"enabled"`
		Threshold int  `mapstructure:"threshold"`
	} `mapstructure:"netmap_cleaner"`

	Contracts struct {
		NeoFS      string `mapstructure:"neofs"`
		Processing string `mapstructure:"processing"`
		Audit      string `mapstructure:"audit"`
		Balance    string `mapstructure:"balance"`
		Container  string `mapstructure:"container"`
		NeoFSID    string `mapstructure:"neofsid"`
		Netmap     string `mapstructure:"netmap"`
		Proxy      string `mapstructure:"proxy"`
		Reputation string `mapstructure:"reputation"`
		Alphabet   struct {
			AZ      string `mapstructure:"az"`
			Buky    string `mapstructure:"buky"`
			Vedi    string `mapstructure:"vedi"`
			Glagoli string `mapstructure:"glagoli"`
			Dobro   string `mapstructure:"dobro"`
			Yest    string `mapstructure:"yest"`
			Zhivete string `mapstructure:"zhivete"`
		} `mapstructure:"alphabet"`
	} `mapstructure:"contracts"`

	Pprof struct {
		Enabled         bool          `mapstructure:"enabled"`
		Address         string        `mapstructure:"address"`
		ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	} `mapstructure:"pprof"`

	Prometheus struct {
		Enabled         bool          `mapstructure:"enabled"`
		Address         string        `mapstructure:"address"`
		ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	} `mapstructure:"prometheus"`

	Settlement struct {
		BasicIncomeRate int64 `mapstructure:"basic_income_rate"`
		AuditFee        int64 `mapstructure:"audit_fee"`
	} `mapstructure:"settlement"`
}
