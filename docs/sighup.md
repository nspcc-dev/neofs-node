# SIGHUP behaviour

## Logger

Logger level can be reloaded with a SIGHUP.

## Prometheus

If any of the fields below are changed, the Prometheus HTTP server is restarted.

```yml
prometheus:
  enabled:
  address:
  shutdown_timeout:
```

## pprof

If any of the fields below are changed, the pprof HTTP server is restarted.

```yml
pprof:
  enabled:
  address:
  shutdown_timeout:
  enable_block:
  enable_mutex:
```

`enable_block` and `enable_mutex` are applied immediately without
restarting the HTTP server.

## Object pool

Worker pool sizes are tuned without restarting the pools.

```yml
object:
  put:
    pool_size_remote:
  search:
    pool_size:

replicator:
  pool_size:
```

## Policer

Available for reconfiguration fields:

```yml
policer:
  head_timeout:
  replication_cooldown:
  object_batch_size:
  max_workers:
  boost_multiplier:
```

## Storage engine

Shards can be added, removed or reloaded with SIGHUP.
Each shard from the configuration is matched with existing shards by
comparing paths from `shard.blobstor` section. After this we have 3 sets:

1. Shards that are missing from the configuration (or have `mode: disabled`) but are currently open.
   These are closed.
2. Shards that are added. These are opened and initialized.
3. Shards that remain in the configuration.
   For these shards we apply reload to a `metabase` and for a mode.

### Metabase

| Changed section | Actions                                                                                                              |
|-----------------|----------------------------------------------------------------------------------------------------------------------|
| `path`          | If `path` is different, metabase is closed and opened with a new path. All other configuration will also be updated. |

## FS chain

| Changed section | Actions                                                                                                                                                                                                 |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `endpoints`     | Updates N3 endpoints.<br/>If new `endpoints` do not contain the endpoint client is connected to, it will reconnect to another endpoint from the new list. Node service can be interrupted in this case. |

## Node

| Changed section | Actions                  |
|-----------------|--------------------------|
| `attribute_*`   | Updates node attributes. |

## Meta service

```yml
fschain:
  endpoints:
```

The meta service re-applies the updated FS chain RPC endpoints.

## gRPC

If any field in the `grpc` section has changed, all public API gRPC servers are
gracefully stopped and restarted with the new configuration. All service
implementations (Object, Container, Session, Accounting, Reputation) are
re-registered automatically in the new servers.

```yml
grpc:
  - endpoint:
    conn_limit:
    tls:
      enabled:
      certificate:
      key:
```

During the restart there is a short period of unavailability.
The control service gRPC endpoint is not affected by SIGHUP.
