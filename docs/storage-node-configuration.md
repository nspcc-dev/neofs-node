# NeoFS Storage node configuration file

This section contains detailed NeoFS Storage node configuration file description
including default config values and some tips to set up configurable values.

There are some custom types used for brevity:
1. `duration` -- string consisting of a number and a suffix. Suffix examples include `s` (seconds), `m` (minutes), `ms` (milliseconds).
2. `size` -- string consisting of a number and a suffix. Suffix examples include `b` (bytes, default), `k` (kibibytes), `m` (mebibytes), `g` (gibibytes).
3. `file mode` -- octal number. Usually, it starts with `0` and contain 3 digits, corresponding to file access permissions for user, group and others.
4. `public key` -- hex-encoded public key
5. `hash160` -- hex-encoded 20-byte hash of a deployed contract.

# Structure

| Section      | Description                                             |
|--------------|---------------------------------------------------------|
| `logger`     | [Logging parameters](#logger-section)                   |
| `pprof`      | [PProf configuration](#pprof-section)                   |
| `prometheus` | [Prometheus metrics configuration](#prometheus-section) |
| `control`    | [Control service configuration](#control-section)       |
| `fschain`    | [N3 blockchain client configuration](#fschain-section)  |
| `apiclient`  | [NeoFS API client configuration](#apiclient-section)    |
| `policer`    | [Policer service configuration](#policer-section)       |
| `replicator` | [Replicator service configuration](#replicator-section) |
| `storage`    | [Storage engine configuration](#storage-section)        |
| `grpc`       | [gRPC configuration](#grpc-section)                     |
| `metadata`   | [Meta service configuration](#meta-section)             |
| `node`       | [Node configuration](#node-section)                     |
| `object`     | [Object service configuration](#object-section)         |


# `control` section
```yaml
control:
  authorized_keys:
    - 035839e45d472a3b7769a2a1bd7d54c4ccd4943c3b40f547870e83a8fcbfb3ce11
    - 028f42cfcb74499d7b15b35d9bff260a1c8d27de4f446a627406a382d8961486d6
  grpc:
    endpoint: 127.0.0.1:8090
```
| Parameter         | Type           | Default value | Description                                                                                                                                                                                |
|-------------------|----------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `authorized_keys` | `[]public key` | empty         | List of public keys which are used to authorize requests to the control service.                                                                                                           |
| `grpc.endpoint`   | `string`       | empty         | Address that control service listener binds to.                                                                                                                                            |

# `grpc` section
```yaml
grpc:
  - endpoint: localhost:8080
    tls:
      enabled: true 
      certificate: /path/to/cert.pem 
      key: /path/to/key.pem
  - endpoint: internal.ip:8080
  - endpoint: external.ip:8080
    tls:
      enabled: true
```
Contains an array of gRPC endpoint configurations. The following table describes the format of each
element.

| Parameter         | Type                          | Default value | Description                                                                                                                   |
|-------------------|-------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------|
| `endpoint`        | `string`                      | empty         | Address that service listener binds to.                                                                                       |
| `conn_limit`      | `int`                         |               | Connection limits. Exceeding connection will not be declined, just blocked before active number decreases or client timeouts. |
| `tls`             | [TLS config](#tls-subsection) |               | Address that control service listener binds to.                                                                               |

## `tls` subsection

| Parameter             | Type     | Default value | Description                                                               |
|-----------------------|----------|---------------|---------------------------------------------------------------------------|
| `enabled`             | `bool`   | `false`       | Address that control service listener binds to.                           |
| `certificate`         | `string` |               | Path to the TLS certificate.                                              |
| `key`                 | `string` |               | Path to the key.                                                          |

# `pprof` section

Contains configuration for the `pprof` profiler.

| Parameter          | Type       | Default value | Description                             |
|--------------------|------------|---------------|-----------------------------------------|
| `enabled`          | `bool`     | `false`       | Flag to enable the service.             |
| `address`          | `string`   |               | Address that service listener binds to. |
| `shutdown_timeout` | `duration` | `30s`         | Time to wait for a graceful shutdown.   |


# `prometheus` section

Contains configuration for the `prometheus` metrics service.

| Parameter          | Type       | Default value | Description                             |
|--------------------|------------|---------------|-----------------------------------------|
| `enabled`          | `bool`     | `false`       | Flag to enable the service.             |
| `address`          | `string`   |               | Address that service listener binds to. |
| `shutdown_timeout` | `duration` | `30s`         | Time to wait for a graceful shutdown.   |

# `logger` section
Contains logger parameters.

```yaml
logger:
  level: info
  encoding: console
  timestamp: true
  sampling:
    enabled: true
```

| Parameter   | Type     | Default value | Description                                                                                         |
|-------------|----------|---------------|-----------------------------------------------------------------------------------------------------|
| `level`     | `string` | `info`        | Logging level.<br/>Possible values:  `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal`   |
| `encoding`  | `string` | `console`     | Logging encoding.<br/>Possible values: `console`, `json`                                            |
| `timestamp` | `bool`   | `false`       | Flag to enable timestamps. If the parameter is not set, they will be enabled when you run with tty. |
| `sampling.enabled` | `bool`   | `false`       | Flag to enable log sampling. |

# `fschain` section

```yaml
fschain:
  dial_timeout: 30s
  cache_ttl: 15s
  endpoints:
    - wss://rpc1.morph.fs.neo.org:40341/ws
    - wss://rpc2.morph.fs.neo.org:40341/ws
 ```

| Parameter              | Type       | Default value       | Description                                                                                                                                                         |
|------------------------|------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dial_timeout`         | `duration` | `1m`                | Timeout for dialing connections to N3 RPCs.                                                                                                                         |
| `cache_ttl`            | `duration` | FS chain block time | Sidechain cache TTL value (min interval between similar calls).<br/>Negative value disables caching.<br/>Cached entities: containers, container lists, eACL tables. |
| `endpoints`            | `[]string` |                     | Ordered array of _webSocket_ N3 endpoint. Only one is connected at a time, the others are for a fallback if any network error appears.                              |
| `reconnections_number` | `int`      | `5`                 | Number of reconnection attempts (through the full list provided via `endpoints`) before RPC connection is considered lost. Non-positive values make no retries.     |
| `reconnections_delay`  | `duration` | `5s`                | Time interval between attempts to reconnect an RPC node from `endpoints` if the connection has been lost.                                                           |

# `storage` section

Local storage engine configuration.

| Parameter                  | Type                           | Default value | Description                                                                                                                                                                                                                                 |
|----------------------------|--------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `shard_pool_size`          | `int`                          | `20`          | Pool size for shard workers. Limits the amount of concurrent `PUT` operations on each shard.                                                                                                                                                |
| `shard_ro_error_threshold` | `int`                          | `0`           | Maximum amount of storage errors to encounter before shard automatically moves to `Degraded` or `ReadOnly` mode.                                                                                                                            |
| `ignore_uninited_shards`   | `bool`                         | `false`       | Flag that specifies whether uninited shards should be ignored.                                                                                                                                                                              |
| `put_retry_deadline`       | `duration`                     | `0`           | If an object cannot be PUT to storage, node tries to PUT it to the best shard for it (according to placement sorting) and only to it for this long before operation error is returned. Defalt value does not apply any retry policy at all. |
| `shard_defaults`           | [Shard config](#shards-config) |               | Configuration for default values in shards.                                                                                                                                                                                                 |
| `shards`                   | [Shard config](#shards-config) |               | Configuration for seprate shards.                                                                                                                                                                                                           |

## `shards` config

Contains configuration of shards.
`shards` subsection contains an array of configurations for each shard.
`shard_defaults` subsection has the same format and specifies defaults for missing values.
The following table describes configuration for each shard.

| Parameter                           | Type                                         | Default value | Description                                                                                                                                                                                                       |
|-------------------------------------|----------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `compress`                          | `bool`                                       | `false`       | Flag to enable compression.                                                                                                                                                                                       |
| `compression_exclude_content_types` | `[]string`                                   |               | List of content-types to disable compression for. Content-type is taken from `Content-Type` object attribute. Each element can contain a star `*` as a first (last) character, which matches any prefix (suffix). |
| `mode`                              | `string`                                     | `read-write`  | Shard Mode.<br/>Possible values:  `read-write`, `read-only`, `degraded`, `degraded-read-only`, `disabled`                                                                                                         |
| `resync_metabase`                   | `bool`                                       | `false`       | Flag to enable metabase resync on start.                                                                                                                                                                          |
| `writecache`                        | [Writecache config](#writecache-subsection)  |               | Write-cache configuration.                                                                                                                                                                                        |
| `metabase`                          | [Metabase config](#metabase-subsection)      |               | Metabase configuration.                                                                                                                                                                                           |
| `blobstor`                          | [Blobstor config](#blobstor-subsection)      |               | Blobstor configuration.                                                                                                                                                                                           |
| `gc`                                | [GC config](#gc-subsection)                  |               | GC configuration.                                                                                                                                                                                                 |

### `blobstor` subsection

Contains storage type and its configuration.
Currently only `fstree` type is supported.

```yaml
blobstor:
  type: fstree
  path: /path/to/blobstor
  perm: 0644
  depth: 1
```

#### Common options for sub-storages
| Parameter                           | Type                   | Default value | Description                                                                                                                                                                                                       |
|-------------------------------------|------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`                              | `string`               |               | Path to the root of the blobstor.                                                                                                                                                                                 |
| `perm`                              | file mode              | `0640`        | Default permission for created files and directories.                                                                                                                                                             |
| `flush_interval`                    | `duration`             | `10ms`        | Time interval between batch writes to disk.                                                                                                                                                                       |

#### `fstree` type options
FSTree stores objects using file system provided by OS. It uses a hierarchy of
directories (tree) based on object ID, directory nesting level is controlled by
depth. To optimize writing performance for small object it can also combine
multiple objects into a single file, this behavior can be controlled as well.

The default FSTree settings are optimized for HDD and small files. In case of
deploying to SSD combined writer is recommended to be disabled completely
(combined_count_limit=1). For medium/large files or smaller drives depth is
recommended to be adjusted to 3 or even lower values. Larger values are only
relevant for big volumes with very high number of stored objects.
| Parameter                 | Type      | Default value | Description                                                                                                                  |
|---------------------------|-----------|---------------|------------------------------------------------------------------------------------------------------------------------------|
| `path`                    | `string`  |               | Path to the root of the blobstor.                                                                                            |
| `perm`                    | file mode | `0640`        | Default permission for created files and directories.                                                                        |
| `depth`                   | `int`     | `4`           | File-system tree depth. Optimal value depends on the number of objects stored in this shard, the number of lower-level directories used by FSTree is 58^depth, with depth 3 this is ~200K, with 4 --- ~11M |
| `no_sync`                 | `bool`    | `false`       | Disable write synchronization, makes writes faster, but can lead to data loss. Not recommended for production use.           |
| `combined_count_limit`    | `int`     | `128`         | Maximum number of objects to write into a single file, 0 or 1 disables combined writing (which is recommended for SSDs).     |
| `combined_size_limit`     | `size`    | `8M`          | Maximum size of a multi-object file.                                                                                         |
| `combined_size_threshold` | `size`    | `128K`        | Minimum size of object that won't be combined with others when writing to disk.                                              |

### `gc` subsection

Contains garbage-collection service configuration. It iterates over the blobstor and removes object the node no longer needs.

```yaml
gc:
  remover_batch_size: 200
  remover_sleep_interval: 5m
```

| Parameter                | Type       | Default value | Description                                  |
|--------------------------|------------|---------------|----------------------------------------------|
| `remover_batch_size`     | `int`      | `100`         | Amount of objects to grab in a single batch. |
| `remover_sleep_interval` | `duration` | `1m`          | Time to sleep between iterations.            | 

### `metabase` subsection

```yaml
metabase:
  path: /path/to/meta.db
  perm: 0644
  max_batch_size: 200
  max_batch_delay: 20ms
```

| Parameter         | Type       | Default value | Description                                                            |
|-------------------|------------|---------------|------------------------------------------------------------------------|
| `path`            | `string`   |               | Path to the metabase file.                                             |
| `perm`            | file mode  | `0640`        | Permissions to set for the database file.                              |
| `max_batch_size`  | `int`      | `1000`        | Maximum amount of write operations to perform in a single transaction. |
| `max_batch_delay` | `duration` | `10ms`        | Maximum delay before a batch starts.                                   |

### `writecache` subsection

```yaml
writecache:
  enabled: true
  path: /path/to/writecache
  capacity: 4294967296
```

| Parameter           | Type       | Default value | Description                                                                                                          |
|---------------------|------------|---------------|----------------------------------------------------------------------------------------------------------------------|
| `enabled`           | `bool`     | `false`       | Flag to enable the writecache.                                                                                       |
| `path`              | `string`   |               | Path to the metabase file.                                                                                           |
| `capacity`          | `size`     | unrestricted  | Approximate maximum size of the writecache. If the writecache is full, objects are written to the blobstor directly. |
| `no_sync`           | `bool`     | `false`       | Disable write synchronization, makes writes faster, but can lead to data loss.                                       |


# `metadata` section

```yaml
metadata:
  path: path/to/meta
```

| Parameter         | Type       | Default value | Description                           |
|-------------------|------------|---------------|---------------------------------------|
| `path`            | `string`   |               | Path to meta data storages, required. |

# `node` section

```yaml
node:
  wallet:
    path: /path/to/wallet.json
    address: NcpJzXcSDrh5CCizf4K9Ro6w4t59J5LKzz
    password: password
  addresses:
    - grpc://external.ip:8082
  attributes:
    - "Price:11"
    - "UN-LOCODE:RU MSK"
    - "key:value"
  relay: false
  persistent_sessions:
    path: /sessions
  persistent_state:
    path: /state
```

| Parameter             | Type                                                          | Default value | Description                                                                                                          |
|-----------------------|---------------------------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------|
| `wallet`              | [Wallet config](#wallet-subsection)                           |               | Wallet configuration.                                                                                                |
| `addresses`           | `[]string`                                                    |               | Addresses advertised in the netmap.                                                                                  |
| `attributes`          | `[]string`                                                    |               | Node attributes as a list of key-value pairs in `<key>:<value>` format. See also docs about verified nodes' domains. |
| `relay`               | `bool`                                                        |               | Enable relay mode.                                                                                                   |
| `persistent_sessions` | [Persistent sessions config](#persistent_sessions-subsection) |               | Persistent session token store configuration.                                                                        |
| `persistent_state`    | [Persistent state config](#persistent_state-subsection)       |               | Persistent state configuration.                                                                                      |


## `wallet` subsection
N3 wallet configuration.

| Parameter  | Type     | Default value | Description                  |
|------------|----------|---------------|------------------------------|
| `path`     | `string` |               | Path to the wallet file.     |
| `address`  | `string` |               | Wallet address to use.       |
| `password` | `string` |               | Password to open the wallet. |

## `persistent_sessions` subsection

Contains persistent session token store configuration. By default sessions do not persist between restarts.

| Parameter | Type     | Default value | Description           |
|-----------|----------|---------------|-----------------------|
| `path`    | `string` |               | Path to the database. |

## `persistent_state` subsection
Configures persistent storage for auxiliary information, such as last seen block height.
It is used to correctly handle node restarts or crashes.

| Parameter | Type     | Default value          | Description            |
|-----------|----------|------------------------|------------------------|
| `path`    | `string` | `.neofs-storage-state` | Path to the database.  |

# `apiclient` section
Configuration for the NeoFS API client used for communication with other NeoFS nodes.

```yaml
apiclient:
  stream_timeout: 20s
  min_connection_time: 20s
  ping_interval: 10s
  ping_timeout: 5s
```
| Parameter           | Type     | Default value | Description                                                          |
|---------------------|----------|---------------|----------------------------------------------------------------------|
| `stream_timeout`    | `duration` | `15s`         | Timeout for individual operations in a streaming RPC.                |
| `min_connection_timeout` | `duration` | `20s` | Minimum time allotted to open connection with remote SNs. |
| `ping_interval` | `duration` | `10s` | Remote SNs ping time interval. |
| `ping_timeout` | `duration` | `10s` | Timeout for remote SN pings. |

# `policer` section

Configuration for the Policer service. It ensures that object is stored according to the intended policy.

```yaml
policer:
  head_timeout: 15s
  replication_cooldown: 100ms
  object_batch_size: 10
  max_workers: 20
```

| Parameter              | Type       | Default value | Description                                         |
|------------------------|------------|---------------|-----------------------------------------------------|
| `head_timeout`         | `duration` | `5s`          | Timeout for performing the `HEAD` operation.        |
| `replication_cooldown` | `duration` | `1s`          | Cooldown time between replication tasks submitting. |
| `object_batch_size`    | `int`      | `10`          | Replication's objects batch size.                   |
| `max_workers`          | `int`      | `20`          | Replication's worker pool's maximum size.           |

# `replicator` section

Configuration for the Replicator service.

```yaml
replicator:
  put_timeout: 15s
  pool_size: 10
```

| Parameter     | Type       | Default value                          | Description                                 |
|---------------|------------|----------------------------------------|---------------------------------------------|
| `put_timeout` | `duration` | `1m`                                   | Timeout for performing the `PUT` operation. |
| `pool_size`   | `int`      | Equal to `object.put.pool_size_remote` | Maximum amount of concurrent replications.  |

# `object` section
Contains object-service related parameters.

```yaml
object:
  put:
    pool_size_remote: 100
```

| Parameter                   | Type  | Default value | Description                                                                                    |
|-----------------------------|-------|---------------|------------------------------------------------------------------------------------------------|
| `delete.tombstone_lifetime` | `int` | `5`           | Tombstone lifetime for removed objects in epochs.                                              |
| `put.pool_size_remote`      | `int` | `10`          | Max pool size for performing remote `PUT` operations. Used by Policer and Replicator services. |
