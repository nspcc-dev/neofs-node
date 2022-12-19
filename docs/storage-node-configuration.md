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
| `contracts`  | [Override NeoFS contracts hashes](#contracts-section)   |
| `morph`      | [N3 blockchain client configuration](#morph-section)    |
| `apiclient`  | [NeoFS API client configuration](#apiclient-section)    |
| `policer`    | [Policer service configuration](#policer-section)       |
| `replicator` | [Replicator service configuration](#replicator-section) |
| `storage`    | [Storage engine configuration](#storage-section)        |


# `control` section
```yaml
control:
  authorized_keys:
    - 035839e45d472a3b7769a2a1bd7d54c4ccd4943c3b40f547870e83a8fcbfb3ce11
    - 028f42cfcb74499d7b15b35d9bff260a1c8d27de4f446a627406a382d8961486d6
  grpc:
    endpoint: 127.0.0.1:8090
```
| Parameter         | Type           | Default value | Description                                                                      |
|-------------------|----------------|---------------|----------------------------------------------------------------------------------|
| `authorized_keys` | `[]public key` | empty         | List of public keys which are used to authorize requests to the control service. |
| `grpc.endpoint`   | `string`       | empty         | Address that control service listener binds to.                                  |

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
      use_insecure_crypto: true
```
Contains an array of gRPC endpoint configurations. The following table describes the format of each
element.

| Parameter                 | Type                          | Default value | Description                                                               |
|---------------------------|-------------------------------|---------------|---------------------------------------------------------------------------|
| `endpoint`                | `[]string`                    | empty         | Address that service listener binds to.                                   |
| `tls`                     | [TLS config](#tls-subsection) |               | Address that control service listener binds to.                           |

## `tls` subsection

| Parameter             | Type     | Default value | Description                                                               |
|-----------------------|----------|---------------|---------------------------------------------------------------------------|
| `enabled`             | `bool`   | `false`       | Address that control service listener binds to.                           |
| `certificate`         | `string` |               | Path to the TLS certificate.                                              |
| `key`                 | `string` |               | Path to the key.                                                          |
| `use_insecure_crypto` | `bool`   | `false`       | If true, ciphers considered insecure by Go stdlib are allowed to be used. |

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
```

| Parameter | Type     | Default value | Description                                                                                       |
|-----------|----------|---------------|---------------------------------------------------------------------------------------------------|
| `level`   | `string` | `info`        | Logging level.<br/>Possible values:  `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal` |

# `contracts` section
Contains override values for NeoFS side-chain contract hashes. Most of the time contract
hashes are fetched from the NNS contract, so this section can be omitted.

```yaml
contracts:
  balance: 5263abba1abedbf79bb57f3e40b50b4425d2d6cd
  container: 5d084790d7aa36cea7b53fe897380dab11d2cd3c
  netmap: 0cce9e948dca43a6b592efe59ddb4ecb89bdd9ca
  reputation: 441995f631c1da2b133462b71859494a5cd45e90
  proxy: ad7c6b55b737b696e5c82c85445040964a03e97f
```

| Parameter    | Type      | Default value | Description               |
|--------------|-----------|---------------|---------------------------|
| `audit`      | `hash160` |               | Audit contract hash.      |
| `balance`    | `hash160` |               | Balance contract hash.    |
| `container`  | `hash160` |               | Container contract hash.  |
| `netmap`     | `hash160` |               | Netmap contract hash.     |
| `reputation` | `hash160` |               | Reputation contract hash. |
| `subnet`     | `hash160` |               | Subnet contract hash.     |

# `morph` section

```yaml
morph:
  dial_timeout: 30s
  cache_ttl: 15s
  rpc_endpoint:
    - address: wss://rpc1.morph.fs.neo.org:40341/ws
      priority: 1
    - address: wss://rpc2.morph.fs.neo.org:40341/ws
      priority: 2
  switch_interval: 2m
 ```

| Parameter         | Type                                                      | Default value    | Description                                                                                                                                                         |
|-------------------|-----------------------------------------------------------|------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dial_timeout`    | `duration`                                                | `5s`             | Timeout for dialing connections to N3 RPCs.                                                                                                                         |
| `cache_ttl`       | `duration`                                                | Morph block time | Sidechain cache TTL value (min interval between similar calls).<br/>Negative value disables caching.<br/>Cached entities: containers, container lists, eACL tables. |
| `rpc_endpoint`    | list of [endpoint descriptions](#rpc_endpoint-subsection) |                  | Array of endpoint descriptions.                                                                                                                                     |
| `switch_interval` | `duration`                                                | `2m`             | Time interval between the attempts to connect to the highest priority RPC node if the connection is not established yet.                                            |

## `rpc_endpoint` subsection
| Parameter  | Type     | Default value | Description                                                                                                                                                                                                              |
|------------|----------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `address`  | `string` |               | _WebSocket_ N3 endpoint.                                                                                                                                                                                                 |
| `priority` | `int`    | `1`           | Priority of an endpoint. Endpoint with a higher priority (lower configuration value) has more chance of being used. Endpoints with equal priority are iterated over randomly; a negative priority is interpreted as `1`. |

# `storage` section

Local storage engine configuration.

| Parameter                  | Type                              | Default value | Description                                                                                                      |
|----------------------------|-----------------------------------|---------------|------------------------------------------------------------------------------------------------------------------|
| `shard_pool_size`          | `int`                             | `20`          | Pool size for shard workers. Limits the amount of concurrent `PUT` operations on each shard.                     |
| `shard_ro_error_threshold` | `int`                             | `0`           | Maximum amount of storage errors to encounter before shard automatically moves to `Degraded` or `ReadOnly` mode. |
| `shard`                    | [Shard config](#shard-subsection) |               | Configuration for separate shards.                                                                               |

## `shard` subsection

Contains configuration for each shard. Keys must be consecutive numbers starting from zero.
`default` subsection has the same format and specifies defaults for missing values.
The following table describes configuration for each shard.

| Parameter                           | Type                                        | Default value | Description                                                                                                                                                                                                       |
|-------------------------------------|---------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `compress`                          | `bool`                                      | `false`       | Flag to enable compression.                                                                                                                                                                                       |
| `compression_exclude_content_types` | `[]string`                                  |               | List of content-types to disable compression for. Content-type is taken from `Content-Type` object attribute. Each element can contain a star `*` as a first (last) character, which matches any prefix (suffix). |
| `mode`                              | `string`                                    | `read-write`  | Shard Mode.<br/>Possible values:  `read-write`, `read-only`, `degraded`, `degraded-read-only`, `disabled`                                                                                                         |
| `resync_metabase`                   | `bool`                                      | `false`       | Flag to enable metabase resync on start.                                                                                                                                                                          |
| `writecache`                        | [Writecache config](#writecache-subsection) |               | Write-cache configuration.                                                                                                                                                                                        |
| `metabase`                          | [Metabase config](#metabase-subsection)     |               | Metabase configuration.                                                                                                                                                                                           |
| `blobstor`                          | [Blobstor config](#blobstor-subsection)     |               | Blobstor configuration.                                                                                                                                                                                           |
| `small_object_size`                 | `size`                                      | `1M`          | Maximum size of an object stored in blobovnicza tree.                                                                                                                                                             |
| `gc`                                | [GC config](#gc-subsection)                 |               | GC configuration.                                                                                                                                                                                                 |

### `blobstor` subsection

Contains a list of substorages each with it's own type.
Currently only 2 types are supported: `fstree` and `blobovnicza`.

```yaml
blobstor:
  - type: blobovnicza
    path: /path/to/blobstor
    depth: 1
    width: 4
  - type: fstree
    path: /path/to/blobstor/blobovnicza
    perm: 0644
    size: 4194304
    depth: 1
    width: 4
    opened_cache_capacity: 50
```

#### Common options for sub-storages
| Parameter                           | Type                                          | Default value | Description                                                                                                                                                                                                       |
|-------------------------------------|-----------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`                              | `string`                                      |               | Path to the root of the blobstor.                                                                                                                                                                                 |
| `perm`                              | file mode                                     | `0660`        | Default permission for created files and directories.                                                                                                                                                             |

#### `fstree` type options
| Parameter           | Type      | Default value | Description                                           |
|---------------------|-----------|---------------|-------------------------------------------------------|
| `path`              | `string`  |               | Path to the root of the blobstor.                     |
| `perm`              | file mode | `0660`        | Default permission for created files and directories. |
| `depth`             | `int`     | `4`           | File-system tree depth.                               |

#### `blobovnicza` type options
| Parameter               | Type      | Default value | Description                                           |
|-------------------------|-----------|---------------|-------------------------------------------------------|
| `path`                  | `string`  |               | Path to the root of the blobstor.                     |
| `perm`                  | file mode | `0660`        | Default permission for created files and directories. |
| `size`                  | `size`    | `1 G`         | Maximum size of a single blobovnicza                  |
| `depth`                 | `int`     | `2`           | Blobovnicza tree depth.                               |
| `width`                 | `int`     | `16`          | Blobovnicza tree width.                               |
| `opened_cache_capacity` | `int`     | `16`          | Maximum number of simultaneously opened blobovniczas. |

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
| `perm`            | file mode  | `0660`        | Permissions to set for the database file.                              |
| `max_batch_size`  | `int`      | `1000`        | Maximum amount of write operations to perform in a single transaction. |
| `max_batch_delay` | `duration` | `10ms`        | Maximum delay before a batch starts.                                   |

### `writecache` subsection

```yaml
writecache:
  enabled: true
  path: /path/to/writecache
  capacity: 4294967296
  small_object_size: 16384
  max_object_size: 134217728
  workers_number: 30
```

| Parameter            | Type       | Default value | Description                                                                                                          |
|----------------------|------------|---------------|----------------------------------------------------------------------------------------------------------------------|
| `path`               | `string`   |               | Path to the metabase file.                                                                                           |
| `capacity`           | `size`     | unrestricted  | Approximate maximum size of the writecache. If the writecache is full, objects are written to the blobstor directly. | 
| `small_object_size`  | `size`     | `32K`         | Maximum object size for "small" objects. This objects are stored in a key-value database instead of a file-system.   |
| `max_object_size`    | `size`     | `64M`         | Maximum object size allowed to be stored in the writecache.                                                          |
| `workers_number`     | `int`      | `20`          | Amount of background workers that move data from the writecache to the blobstor.                                     |
| `max_batch_size`     | `int`      | `1000`        | Maximum amount of small object `PUT` operations to perform in a single transaction.                                  |
| `max_batch_delay`    | `duration` | `10ms`        | Maximum delay before a batch starts.                                                                                 |


# `node` section

```yaml
node:
  wallet:
    path: /path/to/wallet.json
    address: NcpJzXcSDrh5CCizf4K9Ro6w4t59J5LKzz
    password: password
  addresses:
    - grpc://external.ip:8082
  attribute:
    - "Price:11"
    - "UN-LOCODE:RU MSK"
    - "key:value"
  relay: false
  persistent_sessions:
    path: /sessions
  persistent_state:
    path: /state
  subnet:
    exit_zero: false
    entries:
      - 123
  notification:
    enabled: true
    endpoint: tls://localhost:4222
    timeout: 6s
    default_topic: topic
    certificate: /path/to/cert.pem
    key: /path/to/key.pem
    ca: /path/to/ca.pem
```

| Parameter             | Type                                                          | Default value | Description                                                             |
|-----------------------|---------------------------------------------------------------|---------------|-------------------------------------------------------------------------|
| `key`                 | `string`                                                      |               | Path to the binary-encoded private key.                                 |
| `wallet`              | [Wallet config](#wallet-subsection)                           |               | Wallet configuration. Has no effect if `key` is provided.               |
| `addresses`           | `[]string`                                                    |               | Addresses advertised in the netmap.                                     |
| `attribute`           | `[]string`                                                    |               | Node attributes as a list of key-value pairs in `<key>:<value>` format. |
| `relay`               | `bool`                                                        |               | Enable relay mode.                                                      |
| `persistent_sessions` | [Persistent sessions config](#persistent_sessions-subsection) |               | Persistent session token store configuration.                           |
| `persistent_state`    | [Persistent state config](#persistent_state-subsection)       |               | Persistent state configuration.                                         |
| `subnet`              | [Subnet config](#subnet-subsection)                           |               | Subnet configuration.                                                   |
| `notification`        | [Notification config](#notification-subsection)               |               | NATS configuration.                                                     |


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

## `subnet` subsection
This is an advanced section, use with caution.

| Parameter   | Type       | Default value | Description                                          |
|-------------|------------|---------------|------------------------------------------------------|
| `exit_zero` | `bool`     | `false`       | Exit from the default subnet.                        |
| `entries`   | `[]uint32` |               | List of non-default subnet ID this node belongs to.  |

## `notification` subsection
This is an advanced section, use with caution.

| Parameter       | Type       | Default value     | Description                                                       |
|-----------------|------------|-------------------|-------------------------------------------------------------------|
| `enabled`       | `bool`     | `false`           | Flag to enable the service.                                       |
| `endpoint`      | `string`   |                   | NATS endpoint to connect to.                                      |
| `timeout`       | `duration` | `5s`              | Timeout for the object notification operation.                    |
| `default_topic` | `string`   | node's public key | Default topic to use if an object has no corresponding attribute. |
| `certificate`   | `string`   |                   | Path to the client certificate.                                   |
| `key`           | `string`   |                   | Path to the client key.                                           |
| `ca`            | `string`   |                   | Override root CA used to verify server certificates.              |

# `apiclient` section
Configuration for the NeoFS API client used for communication with other NeoFS nodes.

```yaml
apiclient:
  dial_timeout: 15s
  stream_timeout: 20s
  reconnect_timeout: 30s
```
| Parameter         | Type     | Default value | Description                                                           |
|-------------------|----------|---------------|-----------------------------------------------------------------------|
| dial_timeout      | duration | `5s`          | Timeout for dialing connections to other storage or inner ring nodes. |
| stream_timeout    | duration | `15s`         | Timeout for individual operations in a streaming RPC.                 |
| reconnect_timeout | duration | `30s`         | Time to wait before reconnecting to a failed node.                    |

# `policer` section

Configuration for the Policer service. It ensures that object is stored according to the intended policy.

```yaml
policer:
  head_timeout: 15s
```

| Parameter      | Type       | Default value | Description                                  |
|----------------|------------|---------------|----------------------------------------------|
| `head_timeout` | `duration` | `5s`          | Timeout for performing the `HEAD` operation. |

# `replicator` section

Configuration for the Replicator service.

```yaml
replicator:
  put_timeout: 15s
  pool_size: 10
```

| Parameter     | Type       | Default value                          | Description                                 |
|---------------|------------|----------------------------------------|---------------------------------------------|
| `put_timeout` | `duration` | `5s`                                   | Timeout for performing the `PUT` operation. |
| `pool_size`   | `int`      | Equal to `object.put.pool_size_remote` | Maximum amount of concurrent replications.  |

# `object` section
Contains pool sizes for object operations with remote nodes.

```yaml
object:
  put:
    pool_size_remote: 100
```

| Parameter              | Type  | Default value | Description                                                                                    |
|------------------------|-------|---------------|------------------------------------------------------------------------------------------------|
| `put.pool_size_remote` | `int` | `10`          | Max pool size for performing remote `PUT` operations. Used by Policer and Replicator services. |