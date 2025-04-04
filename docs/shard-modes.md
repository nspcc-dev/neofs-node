# Shard modes description

## List of modes

Each mode is characterized by two important properties:
1. Whether modifying operations are allowed.
2. Whether metabase and write-cache is available. 
   The expected deployment scenario is to place both metabase and write-cache on an SSD drive thus these modes
   can be approximately described as no-SSD modes.

| Mode                 | Description                                                                                                                                                                                                                                                                                                                                     |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `read-write`         | Default mode, all operations are allowed.                                                                                                                                                                                                                                                                                                       |
| `read-only`          | Read-only mode, only read operations are allowed, metabase is available.                                                                                                                                                                                                                                                                        |
| `degraded`           | Degraded mode in which metabase and write-cache is disabled. It shouldn't be used at all, because metabase can contain important indices, such as LOCK objects info and modifying operation in this mode can lead to unexpected behaviour. The purpose of this mode is to allow PUT/DELETE operations without the metabase if really necessary. |
| `degraded-read-only` | Same as `degraded`, but with only read operations allowed. This mode is used during SSD replacement and/or when the metabase error counter exceeds threshold.                                                                                                                                                                                   |
| `disabled`           | Currently used only in config file to temporarily disable a shard.                                                                                                                                                                                                                                                                              |

## Transition order

Because each shard consists of multiple components changing its mode is not an atomic operation.
Instead, each component changes its mode independently.

For transitions to `read-write` mode the order is:
1. `metabase`
2. `blobstor`
3. `writecache`

For transitions to all other modes the order is:
1. `writecache`
2. `blobstor`
3. `metabase`

The motivation is to avoid transient errors because write-cache can write to both blobstor and metabase.
Thus, when we want to _stop_ write operations, write-cache needs to change mode before them.
On the other side, when we want to _allow_ them, blobstor and metabase should be writable before write-cache is.

If anything goes wrong in the middle, the mode of some components can be different from the actual mode of a shard.
However, all mode changing operations are idempotent.

## Automatic mode changes

Shard can automatically switch to a `degraded-read-only` mode in 3 cases:
1. If the metabase was not available or couldn't be opened/initialized during shard startup.
2. If shard error counter exceeds threshold.
3. If the metabase couldn't be reopened during SIGHUP handling.