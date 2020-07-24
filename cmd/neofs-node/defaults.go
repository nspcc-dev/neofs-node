package main

import (
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/morph"
	"github.com/spf13/viper"
)

func setDefaults(v *viper.Viper) {
	// Logger section
	{
		v.SetDefault("logger.level", "debug")
		v.SetDefault("logger.format", "console")
		v.SetDefault("logger.trace_level", "fatal")
		v.SetDefault("logger.no_disclaimer", false) // to disable app_name and app_version

		v.SetDefault("logger.sampling.initial", 1000)    // todo: add description
		v.SetDefault("logger.sampling.thereafter", 1000) // todo: add description
	}

	// Transport section
	{
		v.SetDefault("transport.attempts_count", 5)
		v.SetDefault("transport.attempts_ttl", "30s")
	}

	// Peers section
	{
		v.SetDefault("peers.metrics_timeout", "5s")
		v.SetDefault("peers.connections_ttl", "30s")
		v.SetDefault("peers.connections_idle", "30s")
		v.SetDefault("peers.keep_alive.ttl", "30s")
		v.SetDefault("peers.keep_alive.ping", "100ms")
	}

	// Muxer session
	{
		v.SetDefault("muxer.http.read_buffer_size", 0)
		v.SetDefault("muxer.http.write_buffer_size", 0)
		v.SetDefault("muxer.http.read_timeout", 0)
		v.SetDefault("muxer.http.write_timeout", 0)
	}

	// Node section
	{
		v.SetDefault("node.proto", "tcp") // tcp or udp
		v.SetDefault("node.address", ":8080")
		v.SetDefault("node.shutdown_ttl", "30s")
		v.SetDefault("node.private_key", "keys/node_00.key")

		v.SetDefault("node.grpc.logging", true)
		v.SetDefault("node.grpc.metrics", true)
		v.SetDefault("node.grpc.billing", true)

		// Contains public keys, which can send requests to state.DumpConfig
		// for now, in the future, should be replaced with ACL or something else.
		v.SetDefault("node.rpc.owners", []string{
			// By default we add user.key
			// TODO should be removed before public release:
			//      or add into default Dockerfile `NEOFS_NODE_RPC_OWNERS_0=`
			"031a6c6fbbdf02ca351745fa86b9ba5a9452d785ac4f7fc2b7548ca2a46c4fcf4a",
		})
	}

	// Object section
	{
		v.SetDefault("object.max_processing_size", 100) // size in MB, use 0 to remove restriction
		v.SetDefault("object.workers_count", 5)
		v.SetDefault("object.assembly", true)
		v.SetDefault("object.window_size", 3)

		v.SetDefault("object.transformers.payload_limiter.max_payload_size", 5000) // size in KB

		// algorithm used for salt applying in range hash, for now only xor is available
		v.SetDefault("object.salitor", "xor")

		// set true to check container ACL rules
		v.SetDefault("object.check_acl", true)

		v.SetDefault("object.dial_timeout", "500ms")
		rpcs := []string{"put", "get", "delete", "head", "search", "range", "range_hash"}
		for i := range rpcs {
			v.SetDefault("object."+rpcs[i]+".timeout", "5s")
			v.SetDefault("object."+rpcs[i]+".log_errs", false)
		}
	}

	// Replication section
	{
		v.SetDefault("replication.manager.pool_size", 100)
		v.SetDefault("replication.manager.pool_expansion_rate", 0.1)
		v.SetDefault("replication.manager.read_pool_interval", "500ms")
		v.SetDefault("replication.manager.push_task_timeout", "1s")
		v.SetDefault("replication.manager.placement_honorer_enabled", true)
		v.SetDefault("replication.manager.capacities.replicate", 1)
		v.SetDefault("replication.manager.capacities.restore", 1)
		v.SetDefault("replication.manager.capacities.garbage", 1)

		v.SetDefault("replication.placement_honorer.chan_capacity", 1)
		v.SetDefault("replication.placement_honorer.result_timeout", "1s")
		v.SetDefault("replication.placement_honorer.timeouts.put", "5s")
		v.SetDefault("replication.placement_honorer.timeouts.get", "5s")

		v.SetDefault("replication.location_detector.chan_capacity", 1)
		v.SetDefault("replication.location_detector.result_timeout", "1s")
		v.SetDefault("replication.location_detector.timeouts.search", "5s")

		v.SetDefault("replication.storage_validator.chan_capacity", 1)
		v.SetDefault("replication.storage_validator.result_timeout", "1s")
		v.SetDefault("replication.storage_validator.salt_size", 64)              // size in bytes
		v.SetDefault("replication.storage_validator.max_payload_range_size", 64) // size in bytes
		v.SetDefault("replication.storage_validator.payload_range_count", 3)
		v.SetDefault("replication.storage_validator.salitor", "xor")
		v.SetDefault("replication.storage_validator.timeouts.get", "5s")
		v.SetDefault("replication.storage_validator.timeouts.head", "5s")
		v.SetDefault("replication.storage_validator.timeouts.range_hash", "5s")

		v.SetDefault("replication.replicator.chan_capacity", 1)
		v.SetDefault("replication.replicator.result_timeout", "1s")
		v.SetDefault("replication.replicator.timeouts.put", "5s")

		v.SetDefault("replication.restorer.chan_capacity", 1)
		v.SetDefault("replication.restorer.result_timeout", "1s")
		v.SetDefault("replication.restorer.timeouts.get", "5s")
		v.SetDefault("replication.restorer.timeouts.head", "5s")
	}

	// PPROF section
	{
		v.SetDefault("pprof.enabled", true)
		v.SetDefault("pprof.address", ":6060")
		v.SetDefault("pprof.shutdown_ttl", "10s")
		// v.SetDefault("pprof.read_timeout", "10s")
		// v.SetDefault("pprof.read_header_timeout", "10s")
		// v.SetDefault("pprof.write_timeout", "10s")
		// v.SetDefault("pprof.idle_timeout", "10s")
		// v.SetDefault("pprof.max_header_bytes", 1024)
	}

	// Metrics section
	{
		v.SetDefault("metrics.enabled", true)
		v.SetDefault("metrics.address", ":8090")
		v.SetDefault("metrics.shutdown_ttl", "10s")
		// v.SetDefault("metrics.read_header_timeout", "10s")
		// v.SetDefault("metrics.write_timeout", "10s")
		// v.SetDefault("metrics.idle_timeout", "10s")
		// v.SetDefault("metrics.max_header_bytes", 1024)
	}

	// Workers section
	{
		workers := []string{
			"peers",
			"boot",
			"replicator",
			"metrics",
			"event_listener",
		}

		for i := range workers {
			v.SetDefault("workers."+workers[i]+".immediately", true)
			v.SetDefault("workers."+workers[i]+".disabled", false)
			// v.SetDefault("workers."+workers[i]+".timer", "5s") // run worker every 5sec and reset timer after job
			// v.SetDefault("workers."+workers[i]+".ticker", "5s") // run worker every 5sec
		}
	}

	// Morph section
	{

		// Endpoint
		v.SetDefault(
			morph.EndpointOptPath(),
			"http://morph_chain.localtest.nspcc.ru:30333",
		)

		// Dial timeout
		v.SetDefault(
			morph.DialTimeoutOptPath(),
			5*time.Second,
		)

		v.SetDefault(
			morph.MagicNumberOptPath(),
			uint32(netmode.PrivNet),
		)

		{ // Event listener
			// Endpoint
			v.SetDefault(
				morph.ListenerEndpointOptPath(),
				"ws://morph_chain.localtest.nspcc.ru:30333/ws",
			)

			// Dial timeout
			v.SetDefault(
				morph.ListenerDialTimeoutOptPath(),
				5*time.Second,
			)
		}

		{ // Common parameters
			for _, name := range morph.ContractNames {
				// Script hash
				v.SetDefault(
					morph.ScriptHashOptPath(name),
					"c77ecae9773ad0c619ad59f7f2dd6f585ddc2e70", // LE
				)

				// Invocation fee
				v.SetDefault(
					morph.InvocationFeeOptPath(name),
					0,
				)
			}
		}

		{ // Container
			// Set EACL method name
			v.SetDefault(
				morph.ContainerContractSetEACLOptPath(),
				"SetEACL",
			)

			// Get EACL method name
			v.SetDefault(
				morph.ContainerContractEACLOptPath(),
				"EACL",
			)

			// Put method name
			v.SetDefault(
				morph.ContainerContractPutOptPath(),
				"Put",
			)

			// Get method name
			v.SetDefault(
				morph.ContainerContractGetOptPath(),
				"Get",
			)

			// Delete method name
			v.SetDefault(
				morph.ContainerContractDelOptPath(),
				"Delete",
			)

			// List method name
			v.SetDefault(
				morph.ContainerContractListOptPath(),
				"List",
			)
		}

		{ // Netmap
			// AddPeer method name
			v.SetDefault(
				morph.NetmapContractAddPeerOptPath(),
				"AddPeer",
			)

			// New epoch method name
			v.SetDefault(
				morph.NetmapContractNewEpochOptPath(),
				"NewEpoch",
			)

			// Netmap method name
			v.SetDefault(
				morph.NetmapContractNetmapOptPath(),
				"Netmap",
			)

			// Update state method name
			v.SetDefault(
				morph.NetmapContractUpdateStateOptPath(),
				"UpdateState",
			)

			// IR list method name
			v.SetDefault(
				morph.NetmapContractIRListOptPath(),
				"InnerRingList",
			)

			// New epoch event type
			v.SetDefault(
				morph.ContractEventOptPath(
					morph.NetmapContractName,
					morph.NewEpochEventType,
				),
				"NewEpoch",
			)
		}

		{ // Balance
			// balanceOf method name
			v.SetDefault(
				morph.BalanceContractBalanceOfOptPath(),
				"balanceOf",
			)

			// decimals method name
			v.SetDefault(
				morph.BalanceContractDecimalsOfOptPath(),
				"decimals",
			)
		}
	}
}
