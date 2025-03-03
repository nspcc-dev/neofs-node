# SIGHUP behaviour

## Logger

Logger level can be reloaded with a SIGHUP.

## Policer

Available for reconfiguration fields:

```yml
  head_timeout:
  replication_cooldown:
  object_batch_size:
  max_workers:
```

## Storage engine

Shards can be added, removed or reloaded with SIGHUP.
Each shard from the configuration is matched with existing shards by
comparing paths from `shard.blobstor` section. After this we have 3 sets:

1. Shards that are missing from the configuration (or have `mode: disabled`) but are currently open.
   These are closed.
2. Shards that are added. These are opened and initialized.
3. Shards that remain in the configuration.
   For these shards we apply reload to a `metabase` and for a mode. If `resync_metabase` is true, the metabase is also resynchronized.

### Metabase

| Changed section | Actions                                                                                                              |
|-----------------|----------------------------------------------------------------------------------------------------------------------|
| `path`          | If `path` is different, metabase is closed and opened with a new path. All other configuration will also be updated. |

### FS chain

| Changed section | Actions                                                                                                                                                                                                 |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `endpoints`     | Updates N3 endpoints.<br/>If new `endpoints` do not contain the endpoint client is connected to, it will reconnect to another endpoint from the new list. Node service can be interrupted in this case. |

### Node

| Changed section | Actions                  |
|-----------------|--------------------------|
| `attribute_*`   | Updates node attributes. |
