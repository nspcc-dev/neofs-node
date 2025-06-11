# Changelog
Changelog for NeoFS Node

## [Unreleased]

### Added
- Support for announced address specification in IR consensus P2P config (#3361)
- IR node can verify SN with an external validator (#3356)
- SN `logger.sampling.enabled` config option (#3384)
- IR `logger.sampling.enabled` config option (#3384)

### Fixed
- Split object with a missing link object can't be retrieved in some cases (#3337)
- Invalid URI error text in `request create-container` CLI command (#3366)
- Invalid object ID error text in `object delete` CLI command (#3366)
- Old objects without a session token lifetime are not valid (#3373)
- Node attributes are not updated in map after configuration/version change (#3363)
- Node not trying to return to the map without restart once it's out for any reason (#3379)
- Locked object could be deleted from write cache (#3381)
- Update roles event has more parameters after Echidna hard fork (#3393)

### Changed
- Make `MaintenanceModeAllowed` network setting always allowed (#3360)
- Use local BoltDB fork with improved performance (#3372)
- Cache only non-empty netmap (#3378)
- Improved read performance for writecache-enabled configurations (#3380)
- SN now uses cached container SN lists to detect in-container requests for access control (#3385)
- Improved EACL processing performance by avoiding header requests in many cases (#3386)
- SN `fschain.cache_ttl` config defaults to 15s now (#3390)

### Removed
- `blobstor` package (#3371)
- `max_object_size` write cache configuration (#3381)

### Updated
- `github.com/nspcc-dev/neofs-sdk-go` dependency to `v1.0.0-rc.13.0.20250530123548-f8dbe53f3996` (#3373)
- neofs-contracts to 0.23.0 (#3375)
- NeoGo dependency to 0.110.0 (#3392)

### Updating from v0.46.1
Remove `max_object_size` configuration from write caches, it's no longer needed.

## [0.46.1] - 2025-05-16

### Fixed
- Basic income collection/distribution in networks with rate set to zero (#3352)
- Data corruption on replication in some cases (#3353)
- No object address in local HEAD operation error (#3343)
- Excessive number of WebSocket connections to NeoGo RPC nodes for `strict` meta-on-chain policies (#3343)

### Updated
- `github.com/nspcc-dev/neofs-sdk-go` dependency to `v1.0.0-rc.13.0.20250516062127-1f3abc2c67ca` (#3353)

## [0.46.0] - 2025-05-14 - Sorokdo

The main new feature of this release is N3 account support. We have greatly
simplified storage node configuration as well and completed write cache
improvements. "Peapod" substorage is gone as well as many other obsolete
things, please check migration notes for upgrade details. Many internal
improvements and fixes were also done, metabase structure is more compact now
and SearchV2 is used for all appropriate tasks.

### Added
- Expose metrics about write-cache (#3293)
- IR supports recently added Container contract's `create`, `remove` and `putEACL` methods now (#3282)
- SN attempts to make notary requests calling Container contract's `create`, `remove` and `putEACL` methods (#3282)
- SN listens and handles recently added Container contract's `Created` and `Removed` notifications (#3282)
- IR now supports N3 account authorization of Container contract's `create`, `remove` and `putEACL` ops (#3298)
- SN now supports `ObjectService` requests, objects and session/bearer tokens signed using N3 account scheme (#3303, #3305)
- SN attempts to read containers/eACLs calling new Container contract's `getContainerData`/`getEACLData` methods (#3323)
- `request create-container` command to NeoFS CLI (#3319)
- `max_valid_until_block_increment` setting to IR consensus configuration (#3335)

### Fixed
- Bearer token signed not by its issuer is no longer passed (#3216)
- In-object session token signed not by its issuer is no longer passed by SN and CLI (#3216)
- Object signed not by its owner is no longer passed by SN (#3264)
- Metabase indexes cleanup (#3290)
- Missing container status returned by `ContainerService.GetExtendedACL` SN server (#3294)
- Exit code of CLI commands for already removed object (#3300)
- CLI timeout can expire before RPC (#3302)
- `accounting balance` request without timeout (#3302)
- Possible NEO chain client's infinite reconnection loop (#3292)
- Multiple flush same big objects (#3317)
- Split object middle children treated as root objects in SearchV2 (#3316)
- Broken "owner" parameter of "accounting balance" CLI command (#3325)
- Unclosed network connections in CLI (#3326)
- SN no longer fails tombstone verification on `ALREADY_REMOVED` member's status (#3327)
- Panic in search (#3333)
- Insufficient fee for some operations (#3339)
- Race between basic income processing stages leading to distribution failures in some cases (#3338)
- Wallet password logged by IR in some error cases (#3340)
- Embedded CN not stopped properly in some error cases potentially leading to inconsistent blockchain DB state (#3340)
- SN gRPC dialer switched from problematic `dns` address resolver to the `passthrough` one (#3334)

### Changed
- IR calls `ObjectService.SearchV2` to select SG objects now (#3144)
- IR contract approval no longer supports neofsid accounts (#3256)
- IR no longer processes bind/unbind neofsid requests (#3256)
- SN `ExternalAddr` attribute is ignored now (#3235)
- Use list instead of maps for options in node config (#3204, #3341)
- Token and object authentication errors are more detailed in status responses now (#3216, #3264)
- IR uses NNS names for alphabet contracts now instead of Glagolitsa (#3268)
- neofs-adm can handle any wallet names in its configuration (#3268)
- neofs-adm uses alphabet0/1/2 names for wallet files by default now (#3268)
- Distribute load in writecache during flushing (#3261)
- Faster migration in peapod-to-fstree (#3280)
- `peapod-to-fstree` migrates only objects with existing containers (#3283, #3306)
- Storage Node now prohibits parents with their own parents (object split of already split object) (#2451)
- Storage Nodes do not accept REPLICATE with a header that exceeds the limit (#3297)
- Search API is served from SearchV2 indexes now (#3316)
- Blobstor can be of exactly one type, with no substorages (#3330)
- SN uses SearchV2 to verify tombstones (#3312)
- SN handles filtered attribute-less SearchV2 queries faster on big containers (#3329)

### Removed
- SN `apiclient.allow_external` config (#3235)
- Support `morph` name for `fschain` config (#3262)
- All fields from the `contracts` section in IR config, except `contracts.neofs` and `contracts.processing` (#3262)
- Section `contracts` in SN config (#3262)
- `tree` SN configuration section (#3270)
- `pilorama` shard configuration section for SN (#3270)
- Tree service related CLI commands (#3270)
- Netmap v1 node support (#3304)
- `netmap_cleaner` IR configuration (#3304)
- `peapod-to-fstree` migration tool (#3330)
- Peapod type of storage (#3330)
- `small_object_size` shard configuration section for SN (#3330)

### Updated
- `github.com/nspcc-dev/neofs-sdk-go` dependency to `v1.0.0-rc.13.0.20250514113748-9abf8c619246` (#3255, #3345)
- `github.com/nspcc-dev/neofs-contract` dependency to `v0.22.0` (#3282, #3305, #3323)
- `github.com/nspcc-dev/neo-go` dependency to `v0.109.1` (#3298, #3335, #3348)

### Updating from v0.45.2
`apiclient.allow_external` field must be dropped from any SN configuration.
Also, SN `ExternalAddr` attribute is no-op now and should also be removed.

The node config has been changed. Instead of the field `node.attribute_*`,
now there is a list `node.attributes`. Instead of `storage.shard.*`,
there is a list `storage.shards` with configuration of each shard and
`storage.shard.default` now is a `storage.shard_defaults` option with a configuration
of a default values of shards. Please rewrite this fields in your configuration files.

The section `morph` in the config has been renamed to `fschain` in the 0.43.0 release.
Compatibility code has been removed in this release.
Please rename `morph` to `fschain` in your configuration files.

Currently, in IR configuration only 2 fields are supported in the `contracts` section:
`contracts.neofs` and `contracts.processing`. The rest of the contracts,
along with the alphabet, were removed from the config.

In SN configuration section `contracts` is gone and no longer needed.

Experimental tree service is completely removed in this release, please drop
`tree` and `pilorama` configuration sections as well as pilorama DB files.

`netmap_cleaner` section should be removed from IR configuration.

Now the `blobstor` field in the shard configuration in the `storage` section
is not a list of substorages, but a single storage setting. So there is
only one type of storage now. Remove the list from the `blobstor` section in
config and set up one storage.

`small_object_size` section should be removed from shard configuration section for SN.

The deprecated `peapod` storage type and its associated migrator have been fully removed
from the system. All data must be migrated to the new storage type, and support
for peapod is no longer available. Please use `fstree` instead.

## [0.45.2] - 2025-03-25

### Added
- `min_connection_timeout`, `ping_interval` and `ping_timeout` options to 'apiclient' SN config section (#3234)

### Fixed
- NPE in metabase V3->4 migration routine (#3212)
- "new transaction callback finished with error" logs for "already exists in mempool" error (#3218)
- Panic if logger is not set in FSTree (#3220)
- Not initialized FSTree in the write-cache (#3220)
- Potential approval of network map candidate with duplicated attribute by IR (#3224)
- Unscheduled attempt to tick the epoch when new epoch event has already arrived in IR (#3226)
- Missing v2 SN approval from IR for netmap node v1 networks (#3238)
- Collecting of converted attributes in SN `ObjectService.SearchV2` handler (#3230)
- Corrupted data in metabase stops GC cycle and makes problem objects to be kept forever (#3239)
- Requests hanging after SN disappears from the network (#3234)

### Changed
- Retry flush from write-cache after 10s delay if an error is received (#3221)
- SN-SN connection management (#3234)

### Removed
- `dial_timeout` and `reconnect_timeout` options from `apiclient` SN config section (#3234)

### Updated
- Minimum required version of Go to 1.23 (#2918)
- `github.com/nspcc-dev/neofs-sdk-go` dependency to `v1.0.0-rc.13` (#3199)
- `github.com/nspcc-dev/neo-go` dependency to `v0.108.1` (#3199)
- `github.com/nspcc-dev/hrw/v2` dependency to `v2.0.3` (#3199)
- `github.com/stretchr/testify` dependency to `v1.10.0` (#3199)
- `golang.org/x/exp` dependency to `v0.0.0-20250210185358-939b2ce775ac` (#3199)
- `golang.org/x/net` dependency to `v0.32.0` (#3199)
- `golang.org/x/sync` dependency to `v0.11.0` (#3199)
- `google.golang.org/grpc` dependency to `v1.70.0` (#3199)
- `google.golang.org/protobuf` dependency to `v1.36.5` (#3199)

### Updating from v0.45.1
Following SN configurations must be dropped:
- `apiclient.dial_timeout`
- `apiclient.reconnect_timeout`

They are replaced by the next ones:
- `apiclient.min_connection_timeout`
- `apiclient.ping_interval`
- `apiclient.ping_timeout`

These options allow to tune SN-SN connections' keepalive behavior. Defaults
work in most cases, so do not specify any values unless you have a clear need.

## [0.45.1] - 2025-03-07

### Added
- IR `fschain.consensus.p2p_notary_request_payload_pool_size` config option (#3195)
- IR `fschain.consensus.rpc.max_gas_invoke` config option (#3208)

### Fixed
- Zero range denial by `neofs-cli object range|hash` commands (#3182)
- Not all the components needed for write-cache migration were initialized (#3186)
- Homomorphic hash indexes with empty values in meta bucket (#3187)
- Failure of 1st split-chain child processing during metabase migration (#3187)
- Initial epoch tick by a just-started IR (#3007)
- Missing retries when sending transaction/notary request leading to request failure in case of temporary errors (#3193, #3200, #3205)

### Changed
- Use iterators for container listings (#3196)
- Also reloading shard mode with SIGHUP (#3192)
- IR logs INFO its state on start based on read FS chain's internals (#3197)
- IR cache size for handled notary requests (#3205)
- Metabase V3->4 migration routine no longer breaks on broken binary or proto-violating object encounter (#3203)
- Metabase V3->4 migration routine skips objects from the removed containers (#3203)
- Metabase V3->4 migration routine processes up to 1000 objects per BoltDB transaction and keeps the progress (#3202)
- Metabase V3->4 migration routine stops gracefully on OS signal (#3202)

## [0.45.0] - 2025-02-27 - Sapsido

Unleashing the true scaling potential of NeoFS this release is the first one
to break the old ~320 node limit. It also brings a new powerful search API and
distributed settings management commands. Write cache received some care and
improved its efficiency. A number of other optimizations and fixes is included
as well.

### Added
- Initial support for meta-on-chain for objects (#2877)
- First split-object part into the CLI output (#3064)
- `neofs-cli control notary` with `list`, `request` and `sign` commands (#3059)
- IR `fschain.consensus.keep_only_latest_state` and `fschain.consensus.remove_untraceable_blocks` config options (#3093)
- `logger.timestamp` config option (#3105)
- Container system attributes verification on IR side (#3107)
- IR `fschain.consensus.rpc.max_websocket_clients` and `fschain.consensus.rpc.session_pool_size` config options (#3126)
- `ObjectService.SearchV2` SN API (#3111)
- `neofs-cli object searchv2` command (#3119)
- Retries to update node state on new epoch (#3158)

### Fixed
- `neofs-cli object delete` command output (#3056)
- More clear error message when validating IR configuration (#3072)
- Panic during shutdown if N3 client connection is lost (#3073)
- The first object of the split object is not deleted (#3089)
- The parent of the split object is not removed from the metabase (#3089)
- A split expired object is not deleted after the lock is removed or expired (#3089)
- `neofs_node_engine_list_containers_time_bucket` and `neofs_node_engine_exists_time_bucket` metrics (#3014)
- `neofs_node_engine_list_objects_time_bucket` metric (#3120)
- The correct role parameter to invocation (#3127)
- nil pointer error for `storage sanity` command (#3151)
- Process designation event of the mainnet RoleManagement contract (#3134)
- Storage nodes running out of GAS because `putContainerSize` was not paid for by proxy (#3167)
- Write-cache flushing loop to drop objects (#3169)
- FS chain client pool overflows and event ordering (#3163)

### Changed
- Number of cuncurrenly handled notifications from the chain was increased from 10 to 300 for IR (#3068)
- Write-cache size estimations (#3106)
- New network map support solving the limit of ~320 nodes per network (#3088)
- Calculation of VUB for zero hash (#3134)
- More efficient block header subscription is used now instead of block-based one (#3163)
- `ObjectService.GetRange(Hash)` ops now handle zero ranges as full payload (#3071)
- Add some GAS to system fee of notary transactions (#3176)
- IR now applies sane limits to containers' storage policies recently added to the protocol (#3075)
- Make flushing objects in write-cache in parallel (#3179)

### Removed
- Drop creating new eacl tables with public keys (#3096)
- BoltDB from write-cache (#3091)

### Updated
- SDK to the post-api-go version (#3103)
- neofs-contracts to 0.21.0 (#3157)
- NeoGo to 0.108.0 (#3157)

### Updating from v0.44.2
Using public keys as a rule target in eACL tables was deprecated, and
since this realese it is not supported to create new eACL table with keys,
use addresses instead.
For more information call `neofs-cli acl extended create -h`.

`small_object_size`, `workers_number`, `max_batch_size` and `max_batch_delay`
paramteters are removed from `writecache` config. These parameters are related
to the BoltDB part of the write-cache, which is dropped from the code.
Also, because of this, there will be automatic migration from BoltDB by flushing
objects to the main storage and removing database file.

This version maintains two network map lists, the old one is used by default
and the new one will be used once "UseNodeV2" network-wide setting is set to
non-zero value. Storage nodes add their records to both lists by default, so
IR nodes must be updated first, otherwise SNs will fail to bootstrap. Monitor
candidates with neofs-adm and make a switch once all nodes are properly
migrated to the new list.

IR nodes using embedded CN require chain resynchronization (drop the DB to
do that) for this release because of NeoGo updates.

## [0.44.2] - 2024-12-20

### Fixed
- Incomplete metabase migration to version 3 leading to node start failure (#3048)

### Updated
- golang.org/x/crypto dependency from 0.26.0 to 0.31.0 (#3049)

## [0.44.1] - 2024-12-11

### Fixed
- Fail gracefully on error from config validation (#3037)
- False-negative object PUT from container node with set copies number (#3042)
- Metabase resynchronization failure (#3039)

### Changed
- Local object PUT op with copies number set to 1 is allowed now (#3042)
- Number of cuncurrenly handled notifications from the chain was increased from 10 to 100 (#3043)

### Updated
- NeoGo dependency to 0.107.1 (#3040)

## [0.44.0] - 2024-11-28 - Oedo

### Added
- More effective FSTree writer for HDDs, new configuration options for it (#2814)
- New health status `INITIALIZING_NETWORK` in inner ring (#2934)
- IR health status to Prometheus metrics (#2934)
- `neofs-cli control object list` command (#2853)
- `node` config option `storage.ignore_uninited_shards` (#2953)
- `--global-name` flag for `neofs-cli container create` to save container name into `__NEOFS__NAME` and subsequently register it in NNS contract (#2954)
- The last metabase resynchronization epoch into metabase (#2966)
- `neofs-lens meta last-resync-epoch` command (#2966)
- `neofs-lens fstree cleanup-tmp` command (#2967)
- `neofs-cli control object revive` command (#2968)
- `--disable-auto-gen-tag` flag for gendoc command (#2983)
- CLI commands documentation to the `docs/cli-commands` folder (#2983)
- `logger.encoding` config option (#2999)
- Reloading morph endpoints with SIGHUP (#2998)
- New `peapod-to-fstree` tool providing peapod-to-fstree data migration (#3013)
- Reloading node attributes with SIGHUP (#3005)
- Reloading pool sizes with SIGHUP (#3018)
- Reloading pprof/metrics services with SIGHUP (#3016)
- Metrics for shard capacity (#3021)

### Fixed
- Searching (network-wide) for tombstones when handling their expiration, local indexes are used now instead (#2929)
- Unathorized container ops accepted by the IR (#2947)
- Structure table in the SN-configuration document (#2974)
- False negative connection to NeoFS chain in multi-endpoint setup with at least one live node (#2986)
- Overriding the default container and object attributes only with the appropriate flags (#2985)
- RPC client reconnection failures leading to complete SN failure (#2797)
- `meta.DB.Open(readOnly)` moves metabase in RO mode (#3000)
- Panic in event listener related to inability to switch RPC node (#2970)
- Non-container nodes never check placement policy on PUT, SEARCH requests (#3014)
- If shards are overloaded with PUT requests, operation is not skipped but waits for 30 seconds (#2871)
- Data corruption if PUT is done too concurrently (#2978)

### Changed
- `ObjectService`'s `Put` RPC handler caches up to 10K lists of per-object sorted container nodes (#2901)
- Metabase graveyard scheme (#2929)
- When an error is returned, no additional help output is displayed in cobra-based programs (#2942)
- Timestamps are no longer produced in logs if not running with TTY (#2964)
- In inner ring config, default ports for TCP addresses are used if they are missing (#2969)
- Metabase is refilled if an object exists in write-cache but is missing in metabase (#2977)
- Pprof and metrics services stop at the end of SN's application lifecycle (#2976)
- Reject configuration with unknown fields (#2981)
- Log sampling is disabled by default now (#3011)
- EACL is no longer considered for system role (#2972)
- Deprecate peapod substorage (#3013)
- Node does not stop trying to PUT an object if there are more PUT tasks than configured (#3027)
- `morph` configuration section renamed to `fschain` both in IR and SN (#3028)
- FSTree is limited to depth 8 max now (#3031)

### Removed
- Support for node.key configuration (#2959)
- `contracts.alphabet.amount` from inner ring config (#2960)

### Updated
- Go to 1.22 version (#2517, #2738)

### Updating from v0.43.0
Metabase version has been increased, auto migration will be performed once
v0.44.0 Storage Node is started with a v0.43.0 metabase. This action can
not be undone. No additional work should be done.

The new `storage.put_retry_timeout` config value added. If an object cannot
be PUT to storage, node tries to PUT it to the best shard for it (according to
placement sorting) and only to it for this long before operation error is
returned.

Binary keys are no longer supported by storage node, NEP-6 wallet support was
introduced in version 0.22.3 and support for binary keys was removed from
other components in 0.33.0 and 0.37.0. Please migrate to wallets (see 0.37.0
notes) if you've not done it previously.

The section `morph` in the config has been renamed to `fschain`. This version
still supports the old section name, but this compatibility code will be removed
in the next release. Please rename `morph` to `fschain` in your configuration files.

To migrate data from Peapods to FSTree:
```shell
$ peapod-to-fstree -config </path/to/storage/node/config>
```
For any shard, the data from the configured Peapod is copied into an FSTree
that must be already configured. Notice that peapod DB is not deleted during
migration. An updated (peapod-free) configuration file is also created with
".migrated" suffix (for example, `/etc/neofs/config.yaml` -> `/etc/neofs/config.yaml.migrated`).
WARN: carefully review the updated config before using it in the application!

FSTree storage provided with this version is more efficient for small files
than the Peapod in most cases. We support both `fstree` and `peapod` sub-storages,
but `peapod` can be removed in future versions. We recommend using `fstree`.
If you want to use only `fstree` and storage node already stores some data,
don't forget to perform data migration described above.

## [0.43.0] - 2024-08-20 - Jukdo

### Added
- Indexes inspection command to neofs-lens (#2882)
- Add objects sanity checker to neofs-lens (#2506)
- Support for 0.20.0+ neofs-contract archive format (#2872)
- `neofs-cli control object status` command (#2886)
- Check the account alongside the public key in ACL (#2883)
- Allow addresses to be used in EACLs created from CLI (#2914)

### Fixed
- Control service's Drop call does not clean metabase (#2822)
- It was impossible to specify memory amount as "1b" (one byte) in config, default was used instead (#2899)
- Container session token's lifetime was not checked (#2898)
- ACL checks for split objects could be forced by a node than might lack access (#2909)

### Changed
- neofs-cli allows several objects deletion at a time (#2774)
- `ObjectService.Put` server of in-container node places objects using new `ObjectService.Replicate` RPC (#2802)
- `ObjectService`'s `Search` and `Replicate` RPC handlers cache up to 1000 lists of container nodes (#2892)
- Default max_traceable_blocks Morph setting lowered to 17280 from 2102400 (#2897)
- `ObjectService`'s `Get`/`Head`/`GetRange` RPC handlers cache up to 10K lists of per-object sorted container nodes (#2896)

### Updated
- neofs-contract dependency to 0.20.0 (#2872)
- NeoGo dependency to 0.106.3 (#2872)

## [0.42.1] - 2024-06-13

A tiny update that adds compatibility with the Neo N3 Domovoi hardfork.

### Added
- "morph mint-balance" command to neofs-adm (#2867)

### Fixed
- Unenforced IR `morph.consensus.p2p.peers.min` config default (#2856)
- Object parts do not expire (#2858)

### Updated
- NeoGo dependency to 0.106.2 (#2869)

## [0.42.0] - 2024-05-22 - Dokdo

This release adds compatibility with the Neo N3 Cockatrice hardfork, so
while other changes are minor it's still an important update.

### Added
- "storage list" command to neofs-lens (#2852)

### Fixed
- GETRANGE request may fail in certain cases (#2849)

### Changed
- SN API server responds with status message even to old clients from now (#2846)

### Removed
- IR contracts deployment code. Moved to the contracts repo (#2812)
- `blobovnicza-to-peapod` migration utility (#2842)

### Updated
- neofs-contract dependency (#2847)
- NeoGo dependency to 0.106.0 (#2854)

### Updating from v0.41.1
Notice that `blobovnicza-to-peapod` migration utility is gone. Blobovniczas
were removed from the node since 0.39.0, so if you're using any current NeoFS
node version it's not a problem. If you're using 0.38.0 or lower with
blobovniczas configured, please migrate using earlier releases.

## [0.41.1] - 2024-04-27

A set of fixes and small utility improvements. We're providing darwin and
arm64 binaries for you as well now.

### Added
- Container estimations inspector to neofs-adm (#2826)
- Metabase object lister to neofs-lens (#2834)
- Shard ID from metabase reader to neofs-lens (#2834)
- `neofs-cli bearer print` command for reading binary bearer tokens (#2829)
- linux-arm64, darwin-amd64 and darwin-arm64 binaries, linux-arm64 Docker images (#2835)

### Fixed
- Attribute ACL checks for the first split object (#2820)
- Container size estimation contract writing (#2819)
- Custom contract deployment with custom zone via neofs-adm (#2827)
- Errors in neofs-adm morph dump-names output (#2831)
- GC stops if any non-critical "content" errors happen (#2823)
- Endless GC cycle if an object is stored on an unexpected shard (#2821)
- Storage node searches for objects even if local state prohibits operation (#1709)
- First object in a split chain can de deleted (#2839)

## [0.41.0] - 2024-04-19 - Daebudo

A very important release bringing a number of protocol changes. We have not
changed the protocol for more than a year, but now we're doing that to add
support for new functionality as well as fix long-standing issues.

### Added
- Support of numeric object search queries (#2733)
- Support of `GT`, `GE`, `LT` and `LE` numeric comparison operators in CLI (#2733)
- SN eACL processing of NULL and numeric operators (#2742)
- CLI now allows to create and print eACL with numeric filters (#2742)
- gRPC connection limits per endpoint (#1240)
- `neofs-lens object link` command for the new link object inspection (#2799)
- Storage nodes serve new `ObjectService.Replicate` RPC (#2674)

### Fixed
- Access to `PUT` objects no longer grants `DELETE` rights (#2261)
- Storage nodes no longer reject GET w/ TTL=1 requests to read complex objects (#2447)
- Response exceeding the deadline for TLS errors (#2561)
- `neofs-adm morph generate-storage-wallet` was not able to read `--initial-gas` flag (#2766)
- Inter-node connections closed on any status response (#2767)
- Child objects were available for deletion despite any lock relations (#2093)

### Changed
- IR now checks format of NULL and numeric eACL filters specified in the protocol (#2742)
- Empty filter value is now treated as `NOT_PRESENT` op by CLI `acl extended create` cmd (#2742)
- Storage nodes no longer accept objects with header larger than 16KB (#2749)
- IR sends NeoFS chain GAS to netmap nodes every epoch, not per a configurable blocks number (#2777)
- Big objects are split with the new split scheme (#2667, #2782, #2784, #2788, #2792, #2807)
- Background replicator transfers objects using new `ObjectService.Replicate` RPC (#2317)
- Tombstone objects are not allowed to store child objects (incomplete puts are exceptional) (#2810)

### Removed
- Object notifications incl. NATS (#2750)
- Supporting of `__NEOFS__NETMAP*` X-headers (#2751)
- Option to use insecure TLS cipher suites (#2755)
- Counter metrics that were deprecated since v0.38.0 (#2798)

### Updated
- Minimum required version of Go to 1.20 (#2754)
- All dependencies (#2754, #2778, #2787, #2809)

### Updating from v0.40.1
Remove `notification` section from all SN configuration files: it is no longer
supported. All NATS servers running for this purpose only are no longer needed.
If your app depends on notifications transmitted to NATS, do not update and
create an issue please.

Stop attaching `__NEOFS__NETMAP*` X-headers to NeoFS API requests. If your app
is somehow tied to them, do not update and create an issue please.

Notice that this is the last release containing `blobovnicza-to-peapod`
migration utility. Blobovniczas were removed from the node since 0.39.0, so
if you're using any current NeoFS node version it's not a problem. If you're
using 0.38.0 or lower with blobovniczas configured, please migrate ASAP.

Remove `grpc.tls.use_insecure_crypto` from any storage node configuration.

Remove `timers.emit` from any inner ring configuration.

## [0.40.1] - 2024-02-22

### Fixed
- Inability to deploy contract with non-standard zone via neofs-adm (#2740)
- Container session token's `wildcard` field support (#2741)

### Updating from v0.40.0
We no longer provide .tag.gz binaries in releases, they always were just
duplicates, but if you're using them in some scripts please update to fetch
raw binaries. All binaries have OS in their names as well now, following
regular naming used throughout NSPCC, so instead of neofs-cli-amd64 you get
neofs-cli-linux-amd64 now.

CLI command `acl extended create` changed and extended input format for filters.
For example, `attr>=100` or `attr=` are now processed differently. See `-h` for details.

## [0.40.0] - 2024-02-09 - Maldo

### Added
- `neofs-adm morph generate-storage-wallet` now supports more than one wallet generation per call (#2425)
- Missing containers cleanup (#1663)

### Fixed
- IR does not wait for HTTP servers to stop gracefully before exiting (#2704)
- Zero exit code if IR fails (#2704)
- Neo RPC client failure when the first endpoint is unavailable even if there are more endpoints to try (#2703)
- Incorrect address mapping of the Alphabet contracts in NNS produced by deployment procedure (#2713)
- IR compares and logs public keys difference, not hash of keys difference at SN verification check (#2711)
- Incorrect handling of notary request leading to inability to collect signatures in some cases (#2715)
- Deadlock in autodeploy routine (#2720)
- SN now validates session tokens attached to objects (#1159)
- Object search server no longer wastes system resources sending empty intermediate messages to the stream (#2722)

### Changed
- Created files are not group writable (#2589)
- IR does not create new notary requests for the SN's bootstraps but signs the received ones instead (#2717)
- IR can handle third-party notary requests without reliance on receiving the original one (#2715)
- SN validates container session token's issuer to be container's owner (#2466)
- Storage node now consumes much less memory when slicing small data of a fixed size (#2719)

### Removed
- Deprecated `neofs-adm [...] inspect` commands (#2603)

### Updated
- `neo-go` to `v0.105.1` (#2725)

### Updating from v0.39.2
`neofs-adm [...] inspect` commands were deleted, use `get` instead.

## [0.39.2] - 2023-12-21

NeoFS chain's auto-deployment fixes with some maintainability improvements.

### Added
- Logs on a connection drop in the cache of NeoFS API clients (#2694)

### Fixed
- Auto-deployment of the Balance and Container contracts (#2695)

## [0.39.1] - 2023-12-19

### Fixed
- Fund transfer deadlock in NeoFS chain auto-deploy/update procedure (#2681)
- Invalid contracts' update transactions when epochs are stuck during the NeoFS chain update (#2680)
- Metrics availability during startup (#2677)

## [0.39.0] - 2023-12-12 - Baegado

Complete contract autodeploy/autoupdate functionality, much simplified SN/IR
attribute interaction, numerous optimizations and obligatory bug fixes ---
that's your new NeoFS release in short. Beware of deprecated commands and
options removal, check your scripts and configurations and update to the
latest and greatest of NeoFS.

### Added
- Policer's setting to the SN's application configuration (#2600)
- Support of verified domains for the storage nodes (#2280)
- `neofs-lens storage status` CLI command (#2550)
- Human-readable output of objects' creation timestamp to `neofs-cli container list-objects` (#2653)
- Ability to preset P2PNotary and NeoFSAlphabet roles to validators at the FS chain's genesis (#2643)

### Fixed
- `neofs-cli netmap netinfo` documentation (#2555)
- `GETRANGEHASH` to a node without an object produced `GETRANGE` or `GET` requests (#2541, #2598)
- Iteration over locally collected container estimation values (#2597)
- Synchronous objects `PUT` to the local storage (#2607)
- `copies_number` field was not used in `PUT` request handling (#2607)
- Neo-go notary deposit data workaround (#2429)
- Error of missing network endpoint in `neofs-cli object` commands when it is set in the file config (#2608)

### Changed
- FSTree storage now uses more efficient and safe temporary files under Linux (#2566, #2624, #2633)
- BoltDB open timeout increased from 100ms to 1s (#2499)
- Internal container cache size from 10 to 1000 (#2600)
- Transaction witness scope no longer uses CustomGroups relying on more strict Rules now (#2619)
- New optimized SDK version is integrated (#2622)
- IR uses internal LOCODE DB from Go package (#2610, #2658)
- Contract group for the committee is no longer registered/used in the Sidechain auto-deployment (#2642)
- IR does not change SNs' attributes, SNs expand UN/LOCODE attributes do it themselves (#2612)
- The priority of metrics service is increased (#2428)
- The priority of running control service is maximized (#2585)

### Removed
- Deprecated `no-precheck` flag of `neofs-cli container set-eacl` (#2496)
- Recently-handled objects Policer's cache (#2600)
- Deprecated blobovnicza substorage (#2614)
- Contract group wallet support from `neofs-adm` (#2619)
- `neofs-cli util locode generate`and `neofs-cli util locode info` commands (#2610)
- Locode DB configuration options (#2610)
- `v` prefix in version (#2640)

### Updated
- Update minimal supported Go version up to v1.19 (#2485)
- Update `neo-go` to `v0.104.0` (#2221, #2309, #2596, #2626, #2639, #2659)
- `neofs-lens` `inspect` object commands to `get` with `inspect` deprecation (#2548)
- Update `tzhash` to `v1.7.1`
- `github.com/panjf2000/ants/v2` to `v2.8.2`
- `github.com/nspcc-dev/neofs-contract` to `v0.18.0` (#2580)
- `golang.org/x/net` to 0.17.0 (#2621)
- `github.com/nats-io/nats-server/v2` to 2.9.23 (#2623)
- UUID, golang-lru, compress, go-multiaddr, nats.go, cobra, grpc and other dependencies (#2632)
- `hrw` library to v2.0.0 version (#2629, #2658)
- `github.com/nats-io/nkeys` to 0.4.6 (#2636)
- neofs-contract to v0.19.1 (#2659)

### Updating from v0.38.1
Blobovniczas are gone from the node with this release, see 0.38.0 upgrade
notes for migration instruction (the tool is still provided, but will be gone
after a couple of minor releases).

neofs-adm no longer creates a contract group wallet and no longer needs it
for the operation which may affect deployment scripts.

Pre-0.39.0 storage nodes may not be able to register on the network running
0.39.0 inner ring nodes unless they provide complete LOCODE data in the
configuration (0.39.0 SNs add it automatically, so upgrade SNs).

`native_activations` subsection of `consensus` IR configuration should be
removed. It never actually did anything useful anyway.

Automatic contract deployments and updates require `fschain_autodeploy`
configuration, currently it's optional an IR behaves as previously by default.

## [0.38.1] - 2023-09-18

A tiny fix for peapod-enabled nodes.

### Fixed
- Inability to start node with peapods configured (#2576)
- `neofs-adm morph set-config` command error messages (#2556)

## [0.38.0] - 2023-09-13 - Gogado

A number of important fixes are brought by this release, including protocol
ones (subnet and system EACL changes), as well as an updated storage subsystem
and useful CLI updates making it more powerful and user-friendly at the same time.

Some previously deprecated configuration options were removed, so be careful
and this is the last minor release to support the Blobovnicza tree storage
subsystem. Starting with the next minor release, the component will be purged,
so be prepared (see `Updating` section) and migrate to more efficient and
simple Peapod.

### Added
- Embedded Neo contracts in `contracts` dir (#2391)
- `dump-names` command for adm
- `renew-domain` command for adm
- Stored payload metric per container (#2116)
- Stored payload metric per shard (#2023)
- Histogram metrics for RPC and engine operations (#2351)
- New storage component for small objects named Peapod (#2453)
- New `blobovnicza-to-peapod` tool providing blobovnicza-to-peapod data migration (#2453)
- SN's version and capacity is announced via the attributes automatically but can be overwritten explicitly (#2455, #602)
- `peapod` command for `neofs-lens` (#2507)
- New CLI exit code for awaiting timeout (#2380)
- New CLI exit code for already removed objects (#2376)
- Validation of excessive positional arguments to `neofs-cli` commands (#1941)
- `--lifetime` flag to `bearer create` and `object put` CLI commands  (#1574)
- `--expired-at` flag to `session create` and `storagegroup put` CLI commands (#1574)
- Sessions to RPC server running in IR's local consensus mode (#2532)
- `neofs-cli object nodes` command to get SNs for an object (#2512)
- Fetching container estimations via iterators to prevent NeoVM stack overflow (#2173)
- `neofs-adm morph netmap-candidates` CLI command (#1889)
- SN network validation (is available by its announced addresses) on bootstrap by the IR (#2475)
- Display of container alias fee info in `neofs-cli netmap netinfo` (#2553)
- `neofs-lens storage inspect` CLI command (#1336)
- `neofs-lens` payload-only flag (#2543)
- `neofs-lens meta put` CLI command (#1816)
- Sidechain auto-deployment to the Inner Ring app (#2195)

### Fixed
- `neo-go` RPC connection loss handling (#1337)
- Concurrent morph cache misses (#1248)
- Double voting for validators on IR startup (#2365)
- Skip unexpected notary events on notary request parsing step (#2315)
- Session inactivity on object PUT request relay (#2460)
- Missing connection retries on IR node startup when the first configured mainnet RPC node is not in sync (#2474)
- Storage node no longer ignores unhealthy shards on startup (#2464)
- Processing of status errors returned by API client from NeoFS SDK RC-9 (#2465)
- `neofs-lens write-cache list` command duplication (#2505)
- `neofs-adm` works with contract wallet in `init` and `update-contracts` commands only (#2134)
- Missing removed but locked objects in `SEARCH`'s results (#2526)
- LOCK objects and regular objects expiration conflicts (#2392)
- SN responds with a different node information compared to a bootstrapping contract call's argument (#2568)
- `neofs-cli object put` command processes multiple `--attributes` flags (#2427)

### Removed
- Deprecated `morph.rpc_endpoint` SN and `morph.endpoint.client` IR config sections (#2400)
- `neofs_node_object_epoch` metric for IR and SN (#2347)
- Subnets support (#2411)
- Logging utility completely replaced with `zap.Logger` (#696)
- System eACL modification ability in the `neofs-cli` and `IR` (netmap candidate validation) (#2531)

### Changed
- CLI `--timeout` flag configures whole execution timeout from now (#2124)
- CLI default timeout for commands with `--await` flag increased to 1m (#2124)
- BlobStor tries to store object in any sub-storage with free space (#2450)
- SN does not store container estimations in-mem forever (#2472)
- CLI `neofs-cli container set-eacl` checks container's ownership (#2436)

### Updated
- `neofs-sdk-go` to `v1.0.0-rc.11`

### Updating from v0.37.0
CLI command timeouts (flag `--timeout`) now limit the total command execution
time and not single network operation. If any commands suddenly start to fail
on timeout, try increasing the value, for example, twice. Also note that the
execution of commands with the `--await` flag and without an explicitly
specified time period is now limited to 1 minute. This value can be changed with
`--timeout` flag.
Histogram (not counter) RPC/engine operation handling time metrics were added. For
an old engine `*operation_name*_duration` a new `*operation_name*_time` is available.
For an old `*operation_name*_req_duration` RPC a new `rpc_*operation_name*_time` is
available. The old ones (the counters) have been deprecated and will be removed with
the following minor release.
Container estimation main node calculation has been changed. A new 32-byte long
array is taken as a sorting pivot: it is an estimated container with the first 8 bytes
replaced with a target epoch in a little-endian encoding now.

Deprecated `morph.rpc_endpoint` SN and `morph.endpoint.client` IR configurations
have been removed. Use `morph.endpoints` for both instead.
Deprecated `neofs_node_object_epoch` metric for IR and SN (the same for both)
has been removed. Use `neofs_node_state_epoch` for SN and `neofs_ir_state_epoch`
for IR instead.
Deprecated `--no-precheck` flag in `neofs-cli container set-eacl` use `--force` flag instead for skipping validation checks.

Subnets support has been removed:
- IR's `workers.subnet` and `contracts.subnet` configs are not used anymore.
- SN's `node.subnet` config section is not used anymore.
- `neoofs-amd morph` does not have `subnet` subcommand anymore.
- `neofs-cli container create` does not have `--subnet` flag anymore.

Docker images now contain a single executable file and SSL certificates only.

`neofs-cli control healthcheck` exit code is `0` only for "READY" state.

To migrate data from Blobovnicza trees to Peapods:
```shell
$ blobovnicza-to-peapod -config </path/to/storage/node/config>
```
For any shard, the data from the configured Blobovnicza tree is copied into
a created Peapod file named `peapod.db` in the directory where the tree is
located. For example, `/neofs/data/blobovcniza/*` -> `/neofs/data/peapod.db`.
Notice that existing Blobovnicza trees are untouched. Configuration is also
updated, for example, `/etc/neofs/config.yaml` -> `/etc/neofs/config_peapod.yaml`.
WARN: carefully review the updated config before using it in the application!

To store small objects in more effective and simple sub-storage Peapod, replace
`blobovnicza` sub-storage with the `peapod` one in `blobstor` config section.
If storage node already stores some data, don't forget to make data migration
described above.

## [0.37.0] - 2023-06-15 - Sogado

### Added
- `neofs_[node|ir]_version` with `version` label (#2326)
- TLS RPC support for IR consensus mode (#2322)
- Multiple neo-go RPC reconnection attempts with a delay (#2307)

### Changed
- NeoFS CLI generates random private key automatically if wallet is omitted (#2123)
- `morph.validators` config can be omitted for IR in local consensus mode but must be non-empty otherwise (#2311)

### Fixed
- Inability to restore RPC connection after the second disconnect (#2325)
- Tree service panics when cleaning up failed connections (#2335)
- Dropping small objects on any error on write-cache side (#2336)
- Iterating over just removed files by FSTree (#98)
- IR metrics moved to `neofs_ir` namespace and epoch metrics to `object` subsystem (#2344)
- IR metrics config reading (#2344)
- Write-cache initialization in read-only mode (#2504)

### Removed
- Non-notary mode support for sidechain (#2321)
- Priority switching b/w RPC endpoints in the morph client (#2306)
- Support for binary keys in the CLI (#2357)

### Updated
- Update minimal supported Go version up to v1.18 (#2340)
- tzhash library to 1.7.0 (#2348)
- `github.com/hashicorp/golang-lru` to `v2.0.2`
- `neofs-sdk-go` to `v1.0.0-rc.8`
- BoltDB (`go.etcd.io/bbolt`) to 1.3.7

### Updating from v0.36.1
`neofs_node_object_epoch` metric for IR and SN (the same for both) has been deprecated and will be removed with the
next minor release. Use `neofs_node_state_epoch` for SN and `neofs_ir_state_epoch` for IR instead.

Storage and Inner-ring nodes exposes their version via the `neofs_[node|ir]_version` metric now.

In the local consensus mode (IR) it is allowed to provide additional TLS setup addresses now, see
`morph.consensus.rpc.tls` section.
`morph.switch_interval` IR and SN config value is not used anymore.
`morph.rpc_endpoint` SN config value and `morph.endpoint.client` IR config value has been deprecated and will be
removed with the next minor release. Use `morph.endpoints` for both instead (NOTE: it does not have priorities now).

If you're using binary keys with neofs-cli (`-w`), convert them to proper
  NEP-6 wallets like this:
    $ xxd -p < path_to_binary.wallet # outputs hex-encoded key
    $ neofs-cli util keyer <hex_key> # outputs WIF
    $ neo-go wallet import -w <wallet_file> --wif <wif_key>
  or just generate/use new keys.

In local consensus mode `morph.validators` in IR's config can be omitted now, `morph.consensus.committee` will be used instead.
For detached consensus, it is a required config parameter now.

## [0.36.1] - 2023-04-26

### Fixed
- Storage node hanging after RPC disconnect (#2304)
- Using deprecated NeoGo APIs (#2219, #2310)
- Storage node panicking on exit (#2308)
- Inner ring node panic on exit if internal CN is not enabled (#2308)
- Typo in help message (#2240)

### Removed
- FallbackTime Morph client configuration option (#2310)

## [0.36.0] - 2023-04-14 - Gado

A number of fixes and an internal CN for IR nodes. An update is recommended,
data storage scheme is compatible with 0.35.0.

### Added
- Doc for extended headers (#2128)
- Separate batching for replicated operations over the same container in pilorama (#1621)
- `object.delete.tombstone_lifetime` config parameter to set tombstone lifetime in the DELETE service (#2246)
- neofs-adm morph dump-hashes command now also prints NNS domain expiration time (#2295)
- Launch mode of the Inner Ring with local consensus (#2194)

### Changed
- `common.PrintVerbose` prints via `cobra.Command.Printf` (#1962)
- Storage node's `replicator.put_timeout` config default to `1m` (#2227)
- Full list of container is no longer cached (#2176)
- Pilorama now can merge multiple batches into one (#2231)
- Storage engine now can start even when some shard components are unavailable (#2238)
- `neofs-cli` buffer for object put increased from 4 KiB to 3 MiB (#2243)
- `neofs-adm` now reuses network's setting during contract update if they're not overriden by configuration (#2191)

### Fixed
- Pretty printer of basic ACL in the NeoFS CLI (#2259)
- Failing SN and IR transactions because of incorrect scopes (#2230, #2263)
- Global scope used for some transactions (#2230, #2263)
- Potential data loss from nodes outside the container or netmap (#2267)
- Invalid Inner Ring listing method through Netmap contract with notary Sidechain (#2283)
- Divide-by-zero panic in Inner Ring's basic income distribution procedure (#2262)
- Big object removal with non-local parts (#1978)
- Disable pilorama when moving to degraded mode (#2197)
- Fetching blobovnicza objects that not found in write-cache (#2206)
- Do not search for the small objects in FSTree (#2206)
- Correct status error for expired session token (#2207)
- Restore subscriptions correctly on morph client switch (#2212)
- Expired objects could be returned if not marked with GC yet (#2213)
- `neofs-adm morph dump-hashes` now properly iterates over custom domain (#2224)
- Possible deadlock in write-cache (#2239)
- Fix `*_req_count` and `*_req_count_success` metric values (#2241)
- Storage ID update by write-cache (#2244)
- `neo-go` client deadlock on subscription (#2244, #2272)
- Possible panic during write-cache initialization (#2234)
- incorrect NNS resolve handling in neofs-adm (#2296)
- Failed TestNet replication (#2288)

### Updated
- `neo-go` to `v0.101.1` (some code also reworked to not use deprecated APIs)
- `github.com/klauspost/compress` to `v1.15.13`
- `github.com/multiformats/go-multiaddr` to `v0.8.0`
- `golang.org/x/term` to `v0.3.0`
- `google.golang.org/grpc` to `v1.51.0`
- `github.com/nats-io/nats.go` to `v1.22.1`

### Updating from v0.35.0
New experimental config field `object.delete.tombstone_lifetime` allows to set
tombstone lifetime more appropriate for a specific deployment.

## [0.35.0] - 2022-12-28 - Sindo (신도, 信島)

### Added
- `morph list-containers` in `neofs-adm` (#1689)
- `--binary` flag in `neofs-cli object put/get/delete` commands (#1338)
- `session` flag support to `neofs-cli object hash` (#2029)
- Shard can now change mode when encountering background disk errors (#2035)
- Background workers and object service now use separate client caches (#2048)
- `replicator.pool_size` config field to tune replicator pool size (#2049)
- Fix NNS hash parsing in morph client (#2063)
- `neofs-cli neofs-cli acl basic/extended print` commands (#2012)
- `neofs_node_object_*_req_count_success` prometheus metrics for tracking successfully executed requests (#1984)
- Metric 'readonly' to get shards mode (#2022)
- Tree service replication timeout (#2159)
- `apiclient.reconnect_timeout` setting allowing to ignore failed clients for some time (#2164)

### Changed
- `object lock` command reads CID and OID the same way other commands do (#1971)
- `LOCK` object are stored on every container node (#1502)
- `neofs-cli container get-eacl` print ACL table in json format only with arg `--json' (#2012)
- Side chain notary deposits use max uint32 as till parameter (#1486)
- Allow object removal without linking object (#2100)
- `neofs-cli container delete` command pre-checks container ownership (#2106)
- Policer cache size is now 1024 (#2158)
- Tree service now synchronizes with container nodes in a random order (#2127)
- Pilorama no longer tries to apply already applied operations (#2161)
- Use `sync.Pool` in Object.PUT service (#2139)
- Shard uses metabase for `HEAD` requests by default, not write-cache (#2167)
- Clarify help for `--expire-at` parameter for commands `object lock/put` and `bearer create` (#2097)
- Node spawns `GETRANGE` requests signed with the node's key if session key was not found for `RANGEHASH` (#2144)

### Fixed
- Open FSTree in sync mode by default (#1992)
- `neofs-cli container nodes`'s output (#1991)
- Increase error counter for write-cache flush errors (#1818)
- Correctly select the shard for applying tree service operations (#1996)
- Do not panic and return correct errors for bad inputs in `GET_RANGE` (#2007, #2024)
- Physical child object removal by GC (#1699)
- Broadcasting helper objects (#1972)
- `neofs-cli lock object`'s `lifetime` flag handling (#1972)
- Do not move write-cache in read-only mode for flushing (#1906)
- Child object collection on CLI side with a bearer token (#2000)
- Fix concurrent map writes in `Object.Put` service (#2037)
- Malformed request errors' reasons in the responses (#2028)
- Session token's IAT and NBF checks in ACL service (#2028)
- Losing meta information on request forwarding (#2040)
- Assembly process triggered by a request with a bearer token (#2040)
- Losing locking context after metabase resync (#1502)
- Removing all trees by container ID if tree ID is empty in `pilorama.Forest.TreeDrop` (#1940)
- Concurrent mode changes in the metabase and blobstor (#2057)
- Panic in IR when performing HEAD requests (#2069)
- Write-cache flush duplication (#2074)
- Ignore error if a transaction already exists in a morph client (#2075)
- ObjectID signature output in the CLI (#2104)
- Pack arguments of `setPrice` invocation during contract update (#2078)
- `neofs-cli object hash` panic (#2079)
- Closing `neo-go` WS clients on shutdown and switch processes (#2080)
- Making notary deposits with a zero GAS balance (#2080)
- Notary requests on shutdown (#2075)
- `neofs-cli container create ` check the sufficiency of the number of nodes in the selector for replicas (#2038)
- Data duplication during request forwarding (#2047)
- Tree service panic on `TreeMove` operation (#2140)
- Panic in `GETRANGE` with zero length (#2095)
- Spawning useless `GETRANGE` with zero length for a big object (#2101)
- Incomplete object put errors do contain the deepest error's message (#2092)
- Prioritize internal addresses for clients (#2156)
- Force object removal via control service (#2145)
- Synchronizing a tree now longer reports an error for a single-node container (#2154)
- Prevent leaking goroutines in the tree service (#2162)
- Do not search for LOCK objects when delete container when session provided (#2152)
- Race conditions on shard's mode switch (#1956)
- Returning expired/removed objects from write-cache (#2016)

### Removed
- `-g` option from `neofs-cli control ...` and `neofs-cli container create` commands (#2089)
- `--header` from `neofs-cli object get` (#2090)

### Updated
- `neo-go` to `v0.100.0`
- `spf13/cobra` to `v1.6.1`
- `spf13/viper` to `v1.8.0`
- `google.golang.org/grpc` to `v1.50.1`

### Updating from v0.34.0
Pass CID and OID parameters via the `--cid` and `--oid` flags, not as the command arguments.

Replicator pool size can now be fine-tuned with `replicator.pool_size` config field.
The default value is taken from `object.put.pool_size_remote` as in earlier versions.

Added `neofs_node_object_*_req_count_success` metrics for tracking successfully executed requests.

`neofs-cli container delete` command now requires given account or session issuer
to match the container owner. Use `--force` (`-f`) flag to bypass this requirement.

Tree service network replication can now be fine-tuned with `tree.replication_timeout` config field.

## [0.34.0] - 2022-10-31 - Marado (마라도, 馬羅島)

### Added
- `--timeout` flag in `neofs-cli control` commands (#1917)
- Document shard modes of operation (#1909)
- `tree list` CLI command (#1332)
- `TreeService.GetTrees` RPC (#1902)
- All trees synchronization on bootstrap (#1902)
- `--force` flag to `neofs-cli control set-status` command (#1916)
- Logging `SessionService.Create` RPC on the server for debug (#1930)
- Debian packages can now be built with `make debpackage` (#409)

### Changed
- Path to a metabase can now be reloaded with a SIGHUP (#1869)

### Fixed
- `writecache.max_object_size` is now correctly handled (#1925)
- Correctly handle setting ONLINE netmap status after maintenance (#1922)
- Correctly reset shard errors in `ControlService.SetShardMode` RPC (#1931)
- Setting node's network state to `MAINTENANCE` while network settings forbid it (#1916)
- Do not panic during API client creation (#1936)
- Correctly sign new epoch transaction in neofs-adm for a committee of more than 4 nodes (#1949)
- Inability to provide session to NeoFS CLI in a NeoFS-binary format (#1933)
- `neofs-adm` now works correctly with a committee of more than 4 nodes (#1949, #1959)
- Closing a shard now waits until GC background workers stop (#1964)
- Make it possible to use `shard.ContainerSize` in read-only mode (#1975)
- Storage node now starts if at least one gRPC endpoint is available (#1893)
- Panic in API multy client (#1961)
- Blobstor object removal log messages (#1953)
- Missing object relatives in object removal session opened by NeoFS CLI (#1978)
- Bringing a node back online during maintenance (#1900)

### Updated
- `neo-go` to `v0.99.4`
- `protoc` to `v3.21.7`
- `neofs-sdk` to `v1.0.0-rc.7`

### Updating from v0.33.0
Now storage node serves Control API `SetNemapStatus` request with `MAINTENANCE`
status only if the mode is allowed in the network settings. To force starting the local
maintenance on the node, provide `--force` flag to the `neofs-cli control set-status`
command.

## [0.33.0] - 2022-10-17 - Anmado (안마도, 鞍馬島)

### Added
- Serving `NetmapService.NetmapSnapshot` RPC (#1793)
- `netmap snapshot` command of NeoFS CLI (#1793)
- `apiclient.allow_external` config flag to fallback to node external addresses (#1817)
- Support `MAINTENANCE` state of the storage nodes (#1680, #1681)
- Changelog updates CI step (#1808)
- Validate storage node configuration before node startup (#1805)
- `neofs-node -check` command to check the configuration file (#1805)
- `flush-cache` control service command to flush write-cache (#1806)
- `wallet-address` flag in `neofs-adm morph refill-gas` command (#1820)
- Validate policy before container creation (#1704)
- `--timeout` flag in `neofs-cli` subcommands (#1837)
- `container nodes` command to output list of nodes for container, grouped by replica (#1704)
- Configuration flag to ignore shard in `neofs-node` (#1840)
- Add new RPC `TreeService.Healthcheck`
- Fallback to `GET` if `GET_RANGE` from one storage nodes to another is denied by basic ACL (#1884)
- List of shards and logger level runtime reconfiguration (#1770)
- `neofs-adm morph set-config` now supports well-known `MaintenanceModeAllowed` key (#1892)
- `add`, `get-by-path` and `add-by-path` tree service CLI commands (#1332)
- Tree synchronisation on startup (#1329)
- Morph client returns to the highest priority endpoint after the switch (#1615)

### Changed
- Allow to evacuate shard data with `EvacuateShard` control RPC (#1800)
- Flush write-cache when moving shard to DEGRADED mode (#1825)
- Make `morph.cache_ttl` default value equal to morph block time (#1846)
- Policer marks nodes under maintenance as OK without requests (#1680)
- Unify help messages in CLI (#1854)
- `evacuate`, `set-mode` and `flush-cache` control subcommands now accept a list of shard ids (#1867)
- Reading `object` commands of NeoFS CLI don't open remote sessions (#1865)
- Use hex format to print storage node ID (#1765)

### Fixed
- Description of command `netmap nodeinfo` (#1821)
- Proper status for object.Delete if session token is missing (#1697)
- Fail startup if metabase has an old version (#1809)
- Storage nodes could enter the network with any state (#1796)
- Missing check of new state value in `ControlService.SetNetmapStatus` (#1797)
- Correlation of object session to request (#1420)
- Do not increase error counter in `engine.Inhume` if shard is read-only (#1839)
- `control drop-objects` can remove split objects (#1830)
- Node's status in `neofs-cli netmap nodeinfo` command (#1833)
- Child check in object assembly process of `ObjectService.Get` handler (#1878)
- Shard ID in the object counter metrics (#1863)
- Metabase migration from the first version (#1860)

### Removed
- Remove WIF and NEP2 support in `neofs-cli`'s --wallet flag (#1128)
- Remove --generate-key option in `neofs-cli container delete` (#1692)
- Serving `ControlService.NetmapSnapshot` RPC (#1793)
- `control netmap-snapshot` command of NeoFS CLI (#1793)

### Updated

- `neofs-contract` to `v0.16.0`
- `neofs-api-go` to `v2.14.0`

### Updating from v0.32.0
Replace using the `control netmap-snapshot` command with `netmap snapshot` one in NeoFS CLI.
Node can now specify additional addresses in `ExternalAddr` attribute. To allow a node to dial
other nodes external address, use `apiclient.allow_external` config setting.
Add `--force` option to skip placement validity check for container creation.

Pass `maintenance` state to `neofs-cli control set-status` to enter maintenance mode.
If network allows maintenance state (*), it will be reflected in the network map.
Storage nodes under maintenance are not excluded from the network map, but don't
serve object operations. (*) can be fetched from network configuration via
`neofs-cli netmap netinfo` command.

To allow maintenance mode during neofs-adm deployments, set
`network.maintenance_mode_allowed` parameter in config.

When issuing an object session token for root (virtual, "big") objects,
additionally include all members of the split-chain. If session context
includes root object only, it is not spread to physical ("small") objects.

`neofs-node` configuration now supports `mode: disabled` flag for a shard.
This can be used to temporarily ignore shards without completely removing them
from the config file.

## [0.32.0] - 2022-09-14 - Pungdo (풍도, 楓島)

### Added

- Objects counter metric (#1712)
- `meta` subcommand to `neofs-lens` (#1714)
- Storage node metrics with global and per-shard object counters (#1658)
- Removal of trees on container removal (#1630)
- Logging new epoch events on storage node (#1763)
- Timeout for streaming RPC (#1746)
- `neofs-adm` is now able to dump hashes from a custom zone (#1748)
- Empty filename support in the Tree Service (#1698)
- Flag to `neofs-cli container list-objects` command for attribute printing (#1649)

### Changed

- `neofs-cli object put`'s object ID output has changed from "ID" to "OID" (#1296)
- `neofs-cli container set-eacl` command now pre-checks container ACL's extensibility (#1652)
- Access control in Tree service (#1628)
- Tree service doesn't restrict depth in `rpc GetSubTree` (#1753)
- `neofs-adm` registers contract hashes in both hex and string address formats (#1749)
- Container list cache synchronization with the Sidechain (#1632)
- Blobstor components are unified (#1584, #1686, #1523)

### Fixed

- Panic on write-cache's `Delete` operation (#1664)
- Payload duplication in `neofs-cli storagegroup put` (#1706)
- Contract calls in notary disabled environments (#1743)
- `Blobovnicza.Get` op now iterates over all size buckets (#1707)
- Object expiration time (#1670)
- Parser of the placement policy (#1775)
- Tree service timeout logs (#1759)
- Object flushing on writecache side (#1745)
- Active blobovniczas caching (#1691)
- `neofs-adm` TX waiting (#1738)
- `neofs-adm` registers contracts with a minimal GAS payment (#1683)
- Permissions of the file created by `neofs-cli` (#1719)
- `neofs-adm` creates TX with a high priority attribute (#1702)
- Storage node's restart after a hard reboot (#1647)

### Updated

- `neo-go` to `v0.99.2`
- `nspcc-dev/neofs-contract` to `v0.15.5`
- `prometheus/client_golang` to `v1.13.0`
- `google.golang.org/protobuf` to `v1.28.1`

### Updating from v0.31.0

Storage Node now collects object count prometheus metrics: `neofs_node_object_counter`.

Provide `--no-precheck` flag to `neofs-cli container set-eacl` for unconditional sending of a request
(previous default behavior).

## [0.31.0] - 2022-08-04 - Baengnyeongdo (백령도, 白翎島)

### Added

- `neofs-adm` allows deploying arbitrary contracts (#1629)

### Changed

- Priority order in the Morph client (#1648)

### Fixed

- Losing request context in eACL response checks (#1595)
- Do not return expired objects that have not been handled by the GC yet (#1634)
- Setting CID field in `neofs-cli acl extended create` (#1650)
- `neofs-ir` no longer hangs if it cannot bind to the control endpoint (#1643)
- Do not require `lifetime` flag in `session create` CLI command (#1655)
- Using deprecated gRPC options (#1644)
- Increasing metabase error counter on disabled pilorama (#1642)
- Deadlock in the morph client related to synchronous notification handling (#1653)
- Slow metabase `COMMON_PREFIX` search for empty prefix (#1656)

### Removed

- Deprecated `profiler` and `metrics` configuration sections (#1654)

### Updated

- `chzyer/realine` to `v1.5.1`
- `google/uuid` to `v1.3.0`
- `nats-io/nats.go` to `v1.16.0`
- `prometheus/client_golang` to `v1.12.2`
- `spf13/cast` to `v1.5.0`
- `spf13/viper` to `v1.12.0`
- `go.uber.org/zap` to `v1.21.0`
- `google.golang.org/grpc` to `v1.48.0`

### Updating from v0.30.0
Change `morph.endpoint.client` priority values using the following rule:
the higher the priority the lower the value (non-specified or `0` values are
interpreted as the highest priority -- `1`).

Deprecated `profiler` and `metrics` configuration sections are dropped,
use `pprof` and `prometheus` instead.

## [0.30.2] - 2022-08-01

### Added
- `EACL_NOT_FOUND` status code support (#1645).

## [0.30.1] - 2022-07-29

### Fixed

- `GetRange` operation now works correctly with objects stored in write-cache (#1638)
- Losing request context in eACL response checks (#1595)
- Wrong balance contract in innerring processor (#1636)
- `neofs-adm` now sets groups in manifest for all contracts properly (#1631)

### Updated

- `neo-go` to `v0.99.1`
- `neofs-sdk-go` to `v1.0.0-rc.6`

## [0.30.0] - 2022-07-22 - Saengildo (생일도, 生日島)

### Added

- Profiler and metrics services now should be enabled with a separate flag
- Experimental support for the tree-service, disabled by default (#1607)
- Homomorphic hashes calculation can be disabled across the whole network (#1365)
- Improve `neofs-adm` auto-completion (#1594)

### Changed

- Require SG members to be unique (#1490)
- `neofs-cli` now doesn't remove container with LOCK objects without `--force` flag (#1500)
- LOCK objects are now required to have an expiration epoch (#1461)
- `morph` sections in IR and storage node configuration now accept an address and a priority of an endpoint (#1609)
- Morph client now retries connecting to the failed endpoint too (#1609)
- Redirecting `GET` and `GETRANGE` requests now does not store full object copy in memory (#1605)
- `neofs-adm` now registers candidates during initialization in a single transaction (#1608)

### Fixed
- Invalid smart contract address in balance contract listener (#1636)

- Shard now can start in degraded mode if the metabase is unavailable (#1559)
- Shard can now be disabled completely on init failure (#1559)
- Storage group members are now required to be unique (#1490)
- Print shard ID in component logs (#1611)

### Updated
- `neofs-contract` to `v0.15.3`
- `neo-go` to the pre-release version
- `github.com/spf13/cobra` to v1.5.0

### Updating from v0.29.0
Change morph endpoints from simple string to a pair of `address` and `priority`. The second can be omitted.
For inner ring node this resides in `morph.endpoint.client` section,
for storage node -- in `morph.rpc_endpoint` section. See `config/example` for an example.

Move `storage.default` section to `storage.shard.default`.

Rename `metrics` and `profiler` sections to `prometheus` and `pprof` respectively, though old versions are supported.
In addition, these sections must now be explicitly enabled with `enabled: true` flag.

## [0.29.0] - 2022-07-07 - Yeonpyeongdo (연평도, 延坪島)

Support WalletConnect signature scheme.

### Added
- Retrieve passwords for storage wallets from the configuration in neofs-adm (#1539)
- Metabase format versioning (#1483)
- `neofs-adm` generates wallets in a pretty JSON format
- `Makefile` supports building from sources without a git repo

### Fixed
- Do not replicate object twice to the same node (#1410)
- Concurrent object handling by the Policer (#1411)
- Attaching API version to the forwarded requests (#1581)
- Node OOM panics on `GetRange` request with extremely huge range length (#1590)

### Updated
- `neofs-sdk-go` to latest pre-release version
- `tzhash` to `v1.6.1`

## [0.28.3] - 2022-06-08

### Updated
- Neo-go 0.98.3 => 0.99.0 (#1480)

### Changed
- Replace pointers with raw structures in results for local storage (#1460)
- Move common CLI's functions in a separate package (#1452)

### Fixed
- Confirmation of eACL tables by alphabet nodes when ACL extensibility is disabled (#1485)
- Do not use WS neo-go client in `neofs-adm` (#1378)
- Log more detailed network errors by the Alphabet (#1487)
- Fix container verification by the Alphabet (#1464)
- Include alphabet contracts to the base group in `neofs-adm` (#1489)

## [0.28.2] - 2022-06-03

### Updated
- Neo-go 0.98.2 => 0.98.3 (#1430)
- NeoFS SDK v1.0.0-rc.3 => v1.0.0-rc.4
- NeoFS API v2.12.1 => v2.12.2
- NeoFS Contracts v0.14.2 => v0.15.1

### Added
- Config examples for Inner ring application (#1358)
- Command for documentation generation for `neofs-cli`, `neofs-adm` and `neofs-lens` (#1396)

### Fixed
- Do not ask for contract wallet password twice (#1346)
- Do not update NNS group if the key is the same (#1375)
- Make LOCODE messages more descriptive (#1394)
- Basic income transfer's incorrect log message (#1374)
- Listen to subnet removal events in notary-enabled env (#1224)
- Update/remove nodes whose subnet has been removed (#1162)
- Potential removal of local object when policy isn't complied (#1335)
- Metabase `Select` is now slightly faster (#1433)
- Fix a number of bugs in writecache (#1462)
- Refactor eACL processing and fix bugs (#1471)
- Do not validate subnet removal by IR (#1441)
- Replace pointers with raw structures in parameters for local storage (#1418)

#### Removed
- Remove `num` and `shard_num` parameters from the configuration (#1474)

## [0.28.1] - 2022-05-05

### Fixed
- Loss of the connection scheme during address parsing in NeoFS CLI (#1351)

## [0.28.0] - 2022-04-29 - Heuksando (흑산도, 黑山島)

### Added

- `morph dump-balances` command to NeoFS Adm (#1308)
- Ability to provide session token from file in NeoFS CLI (#1216)

### Fixed

- Panic in `netmap netinfo` command of NeoFS CLI (#1312)
- Container cache invalidation on DELETE op (#1313)
- Subscription to side-chain events in shards (#1321)
- Trusted object creation without session token (#1283)
- Storing invalid objects during trusted PUT op (#1286)
- RAM overhead when writing objects to local storage (#1343)

### Changed

- NeoFS Adm output from stderr to stdout (#1311)
- Node's object GC mechanism (#1318)

### Updating from v0.28.0-rc.3
Clean up all metabases and re-sync them using `resync_metabase` config flag.

## [0.28.0-rc.3] - 2022-04-08

### Fixed
- Check expiration epoch of provided session token (#1168)
- Prevent corruption in `writecache.Head` (#1149)
- Use separate caches in N3 RPC multi client (#1213)
- `neofs-adm` fixes (#1288, #1294, #1295)
- Don't stop notification listener twice (#1291)
- Metabase panic (#1293)
- Disallow to tick block timer twice on the same height (#1208)

### Added
- Persistent storage for session tokens (#1189)
- Cache for Inner Ring list fetcher (#1278)
- Degraded mode of storage engine (#1143)
- Command to change native Policy contract in `neofs-adm` (#1289)
- Single websocket endpoint pool for RPC and notifications (#1053)

### Changed
- Cache NeoFS clients based only on public key (#1157)
- Make `blobovnicza.Put` idempotent (#1262)
- Optimize metabase list operations (#1262)
- PDP check ranges are now asked in random order (#1163)
- Update go version up to v1.17 (#1250)

### Removed
- Reduced amount of slices with pointers (#1239)

### Updating from v0.28.0-rc.2
Remove `NEOFS_IR_MAINNET_ENDPOINT_NOTIFICATION`,
`NEOFS_IR_MORPH_ENDPOINT_NOTIFICATION`, and `NEOFS_MORPH_NOTIFICATION_ENDPOINT`
from Inner Ring and Storage configurations.

Specify _WebSocket_ endpoints in `NEOFS_IR_MAINNET_ENDPOINT_CLIENT`,
`NEOFS_IR_MORPH_ENDPOINT_CLIENT`, and `NEOFS_MORPH_RPC_ENDPOINT` at Inner Ring
and Storage configurations.

Specify path to persistent session token db in Storage configuration with
`NEOFS_NODE_PERSISTENT_SESSIONS_PATH`.

## [0.28.0-rc.2] - 2022-03-24

### Fixed
- Respect format flags for `SplitInfo` output (#1233)
- Output errors in neofs-cli to stderr where possible (#1259)

### Added
- Print details for AccessDenied errors in neofs-cli (#1252)
- Split client creation into 2 stages (#1244)
- Update morph client to work with v0.15.0 (#1253)

## [0.28.0-rc.1] - 2022-03-18

Native RFC-6979 signatures of messages and tokens, LOCK object types,
experimental notifications over NATS with NeoFS API v2.12 support

### Fixed
- Remove session tokens from local storage of storage node after expiration (#1133)
- Readme typos (#1167)
- LOCODE attribute and announced address are not mandatory for relay node config (#1114)
- Check session token verb (#1191)
- Fix data race leading to reputation data loss (#1210)

### Added
- Look for `CustomGroup` scope in NNS contract before contract invocation (#749)
- Cache of notary transaction heights (#1151)
- NATS notifications (#1183)
- LOCK object type (#1175, #1176, #1181)
- Progress bar for object upload/download in neofs-cli (#1185)
- Support of new status codes (#1247)

### Changed
- Update neofs-api-go and neofs-sdk-go (#1101, #1131, #1195, #1209, #1231)
- Use `path/filepath` package for OS path management (#1132)
- Shard sets mode to `read-only` if it hits threshold limit (#1118)
- Use request timeout in chain client of neofs-adm (#1115)
- Generate wallets with 0644 permissions in neofs-adm (#1115)
- Use cache of parsed addresses in GC (#1115)
- Determine config type based on file extension in neofs-ir (#1115)
- Reuse some error defined in contracts (#1115)
- Improved neofs-cli usability (#1103)
- Refactor v2 / SDK packages in eACL (#596)

### Removed
- Remove some wrappers from `morph` package (#625)
- `GetRange` method in blobovnicza (#1115)
- Deprecated structures from SDK v1.0.0 rc (#1181)

### Updating from neofs-node v0.27.5
Set shard error threshold for read-only mode switch with
`NEOFS_STORAGE_SHARD_RO_ERROR_THRESHOLD` (default: 0, deactivated).

Set NATS configuration for notifications in `NEOFS_NODE_NOTIFICATION` section.
See example config for more details.

## [0.27.7] - 2022-03-30

### Fixed
- Shard ID is now consistent between restarts (#1204)

### Added
- More N3 RPC caches in object service (#1278)

## [0.27.6] - 2022-03-28

### Fixed
- Allow empty passwords in neofs-cli config (#1136)
- Set correct audit range hash type in neofs-ir (#1180)
- Read objects directly from blobstor in case of shard inconsistency (#1186)
- Fix `-w` flag in subnet commands of neofs-adm (#1223)
- Do not use explicit mutex lock in chain caches (#1236)
- Force gRPC server stop if it can't shut down gracefully in storage node (#1270)
- Return non-zero exit code in `acl extended create` command failures and fix
  help message (#1259)

### Added
- Interactive storage node configurator in neofs-adm (#1090)
- Logs in metabase operations (#1188)

## [0.27.5] - 2022-01-31

### Fixed
- Flush small objects when persist write cache (#1088)
- Empty response body in object.Search request (#1098)
- Inner ring correctly checks session token in container.SetEACL request (#1110)
- Printing in verbose mode in CLI (#1120)
- Subnet removal event processing (#1123)

### Added
- Password support in CLI config (#1103)
- Shard dump restore commands in CLI (#1085, #1086)
- `acl extended create` command in CLI (#1092)

### Changed
- Adopt new `owner.ID` API from SDK (#1100)
- Use `go install` instead of `go get` in Makefile (#1102)
- Storage node returns Fixed12 decimal on accounting.Balance request. CLI
  prints Fixed8 rounded value by default. (#1084)
- Support new update interface for NNS contract in NeoFS Adm (#1091)
- Rename `use_write_cache` to `writecache.enabled` in stoarge config (#1117)
- Preallocate slice in `headersFromObject` (#1115)
- Unify collection of expired objects (#1115)
- Calculate blobovnicza size at initialization properly (#1115)
- Process fast search filters outside bbolt transaction (#1115)
- Update TZHash library to v1.5.1

### Removed
- `--wif` and `--binary-key` keys from CLI (#1083)
- Extended ACL validator moved to SDK library (#1096)
- `--generate-key` flag in CLI control commands (#1103)
- Various unused code (#1123)

### Updating from v0.27.4
Use `--wallet` key in CLI to provide WIF or binary key file instead of `--wif`
and `--binary-key`.

Replace `NEOFS_STORAGE_SHARD_N_USE_WRITE_CACHE` with
`NEOFS_STORAGE_SHARD_N_WRITECACHE_ENABLED` in Storage node config.

Specify `password: xxx` in config file for NeoFS CLI to avoid password input.

## [0.27.4] - 2022-01-13

### Fixed
- ACL check did not produce status code (#1062)
- Asset transfer wrapper used incorrect receiver (#1069)
- Empty search response missed meta header and body (#1063)
- IR node in single chain environment used incorrect source of IR list (#1025)
- Incorrect message sequence in object.Range request (#1077)

### Added
- Option to disable compression of object based on their content-type attribute
  (#1060)

### Changed
- Factor out autocomplete command in CLI and Adm (#1041)
- Single crypto rand source (#851)

### Updating from v0.27.3
To disable compression for object with specific content-types, specify them
as a string array in blobstor section:
`NEOFS_STORAGE_SHARD_N_BLOBSTOR_COMPRESSION_EXCLUDE_CONTENT_TYPES`. Use
asterisk as wildcard, e.g. `video/*`.

## [0.27.3] - 2021-12-30

### Added
- `SetShardMode` RPC in control API, available in CLI (#1044)
- Support of basic ACL constants without final flag in CLI (#1066)

### Changed
- `neofs-adm` updates contracts in single tx (#1035)
- Proxy contract arguments for deployment in `neofs-adm` (#1056)

## [0.27.2] - 2021-12-28

### Fixed
- Goroutine leak due to infinite response message await ([neofs-api-go#366](https://github.com/nspcc-dev/neofs-api-go/pull/366))
- Inconsistency in placement function ([neofs-sdk-go#108](https://github.com/nspcc-dev/neofs-sdk-go/pull/108))

### Added
- `ListShards` RPC in control API, available in CLI (#1043)
- Epoch metric in Storage and Inner Ring applications (#1054)

### Changed
- Some object replication related logs were moved to DEBUG level (#1052)

## [0.27.1] - 2021-12-20

### Fixed
- Big objects now flushed from WriteCache after write (#1028)
- WriteCache big object counter (#1022)
- Panic in the container estimation routing (#1016)
- Shutdown freeze in policer component (#1047)

### Added
- Shorthand `-g` for `--generate-key` in NeoFS CLI (#1034)
- Autocomplete generator command for neofs-adm (#1013)
- Max connection per host config value for neo-go client (#780)
- Sanity check of session token context in container service (#1045)

### Changed
- CLI now checks NeoFS status code for exit code (#1039)
- New `Update` method signature for NNS contract in neofs-adm (#1038)

## [0.27.0] - 2021-12-09 - Sinjido (신지도, 薪智島)

NeoFS API v2.11.0 support with response status codes and storage subnetworks.

### Fixed
- CLI now opens LOCODE database in read-only mode for listing command (#958)
- Tombstone owner now is always set (#842)
- Node in relay mode does not require shard config anymore (#969)
- Alphabet nodes now ignore notary notifications with non-HALT main tx (#976)
- neofs-adm now prints version of NNS contract (#1014)
- Possible NPE in blobovnicza (#1007)
- More precise calculation of blobovnicza size (#915)

### Added
- Maintenance mode for Storage node (#922)
- Float values in Storage node config (#903)
- Status codes for NeoFS API Response messages (#961)
- Subnetwork support (#977, #973, #983, #974, #982, #979, #998, #995, #1001, #1004)
- Customized fee for named container registration (#1008)

### Changed
- Alphabet contract number is not mandatory (#880)
- Alphabet nodes resign `AddPeer` request if it updates Storage node info (#938)
- All applications now use client from neofs-sdk-go library (#966)
- Some shard configuration records were renamed, see upgrading section (#859)
- `Nonce` and `VUB` values of notary transactions generated from notification
  hash (#844)
- Non alphabet notary invocations now have 4 witnesses (#975)
- Object replication is now async and continuous (#965)
- NeoFS ADM updated for the neofs-contract v0.13.0 deploy (#984)
- Minimal TLS version is set to v1.2 (#878)
- Alphabet nodes now invoke `netmap.Register` to add node to the network map
  candidates in notary enabled environment (#1008)

### Updating from v0.26.1
`NEOFS_IR_CONTRACTS_ALPHABET_AMOUNT` is not mandatory env anymore. If it
is not set, Inner Ring would try to read maximum from config and NNS contract.
However, that parameter still can be set in order to require the exact number
of contracts.

Shard configuration records were renamed:
- `refill_metabase` -> `resync_metabase`
- `writecache.max_size` -> `writecache.max_object_size`
- `writecache.mem_size` -> `writecache.memcache_capacity`
- `writecache.size_limit` -> `writecache_capcity`
- `blobstor.blobovnicza.opened_cache_size` -> `blobstor.blobovnicza.opened_cache_capacity`
- `*.shallow_depth` -> `*.depth`
- `*.shallow_width` -> `*.width`
- `*.small_size_limit` -> `*.small_object_size`

Specify storage subnetworks in `NEOFS_NODE_SUBNET_ENTRIES` as the list of
integer numbers. To exit default subnet, use `NEOFS_NODE_SUBNET_EXIT_ZERO=true`

Specify fee for named container registration in notary disabled environment
with `NEOFS_IR_FEE_NAMED_CONTAINER_REGISTER`.

## [0.26.1] - 2021-11-02

### Fixed
- Storage Node handles requests before its initialization is finished (#934)
- Release worker pools gracefully (#901)
- Metabase ignored containers of storage group and tombstone objects
  in listing (#945)
- CLI missed endpoint flag in `control netmap-snapshot` command (#942)
- Write cache object persisting (#866)

### Added
- Quote symbol support in `.env` example tests (#935)
- FSTree object counter (#821)
- neofs-adm prints contract version in `dump-hashes` command (#940)
- Default values section in shard configuration (#877)
- neofs-adm downloads contracts directly from GitHub (#733)

### Changed
- Use FSTree counter in write cache (#821)
- Calculate notary deposit `till` parameter depending on available
  deposit (#910)
- Storage node returns session token error if attached token's private key
  is not available (#943)
- Refactor of NeoFS API client in inner ring (#946)
- LOCODE generator tries to find the closest continent if there are
  no exact match (#955)

### Updating from v0.26.0
You can specify default section in storage engine configuration.
See [example](./config/example/node.yaml) for more details.

## [0.26.0] - 2021-10-19 - Udo (우도, 牛島)

NeoFS API v2.10 support

### Fixed
- Check remote node public key in every response message (#645)
- Do not lose local container size estimations (#872)
- Compressed and uncompressed objects are always available for reading
  regardless of compression configuration (#868)
- Use request session token in ACL check of object.Put (#881)
- Parse URI in neofs-cli properly (#883)
- Parse minutes in LOCODE DB properly (#902)
- Remove expired tombstones (#884)
- Close all opened blobovniczas properly (#896)
- Do not accept objects with empty OwnerID field (#841)

### Added
- More logs in governance and policer components (#867, #882)
- Contract address getter in static blockchain clients (#627)
- Alphabet configuration option to disable governance sync (#869)
- neofs-lens app implementation (#791)
- Detailed comments in neofs-node config example (#858)
- Size suffixes support in neofs-node config (#857)
- Docs for neofs-adm (#906)
- Side chain block size duration and global NeoFS configuration in
  NetworkConfig response (#833)
- Support native container names (#889)

### Changed
- Updated grpc to v1.41.0 (#860)
- Updated neo-go to v0.97.3 (#833)
- Updated neofs-api-go to v1.30.0
- Adopt neofs-adm for new contracts release (#835, #888)
- Adopt neofs-node for new contracts release (#905)
- SN and IR notary deposits are made dynamically depending on the Notary and
  GAS balances (#771)
- VMagent port in testnet config is now 443 (#908)
- Use per-shard worker pools for object.Put operations (#674)
- Renamed `--rpc-endpoint` CLI flag for `control command` to `--endpoint` (#879)

### Removed
- Global flags in CLI. Deleted useless flags from `accounting balance`
  command (#810).
- Interactive mode in docker run command (#916)

### Updating from v0.25.1
Deleted `NEOFS_IR_NOTARY_SIDE_DEPOSIT_AMOUNT`, `NEOFS_IR_NOTARY_MAIN_DEPOSIT_AMOUNT`
and `NEOFS_IR_TIMERS_SIDE_NOTARY`, `NEOFS_IR_TIMERS_MAIN_NOTARY` Inner Ring envs.
Deleted `NEOFS_MORPH_NOTARY_DEPOSIT_AMOUNT` and `NEOFS_MORPH_NOTARY_DEPOSIT_DURATION`
Storage Node envs.

`control` CLI command does not have `--rpc-endpoint`/`r` flag, use `endpoint`
instead.

## [0.25.1] - 2021-09-29

### Fixed
- Panic caused by missing Neo RPC endpoints in storage node's config (#863)

### Added
- Support of multiple Neo RPC endpoints in Inner Ring node (#792)

`mainchain` section of storage node config is left unused by the application.

## [0.25.0] - 2021-09-27 - Mungapdo (문갑도, 文甲島)

### Fixed
- Work of a storage node with one Neo RPC endpoint instead of a list (#746)
- Lack of support for HEAD operation on the object write cache (#762)
- Storage node attribute parsing is stable now (#787)
- Inner Ring node now logs transaction hashes of Deposit and Withdraw events
  in LittleEndian encoding (#794)
- Storage node uses public keys of the remote nodes in placement traverser
  checks (#645)
- Extended ACL `Target` check of role and public keys is mutual exclusive now
  (#816)
- neofs-adm supports update and deploy of neofs-contract v0.11.0 (#834, #836)
- Possible NPE in public key conversion (#848)
- Object assembly routine do not forward existing request instead of creating
  new one (#839)
- Shard now returns only physical stored objects for replication (#840)

### Added
- Support events from P2P notary pool
- Smart contract address auto negotiation with NNS contract (#736)
- Detailed logs for all data writing operations in storage engine (#790)
- Docker build and release targets in Makefile (#785)
- Metabase restore option in the shard config (#789)
- Write cache used size limit in bytes (#776)

### Changed
- Reduce container creation delay via listening P2P notary pool (#519)
- Extended ACL table is not limited to 1KiB (#731)
- Netmap side chain client wrapper now has `TryNotary` option (#793)
- Sticky bit is ignored in requests with `SYSTEM` role (#818)
- Incomplete object put error now contains last RPC error (#778)
- Container service invalidates container cache on writing operations (#803)
- Improved write cache size counters (#776)
- Metabase returns `NotFound` error instead of `AlreadyRemoved` on GCMarked
  objects (#840)
- Object service uses two routine pools for remote and local GET requests (#845)

### Removed
- Dockerfile for AllInOne image moved to a separate repository (#796)

### Updating from v0.24.1
Added `NEOFS_CONTRACTS_PROXY` env for Storage Node; mandatory in
notary enabled environments only. It should contain proxy contract's
scripthash in side chain.

Added `NEOFS_MORPH_NOTARY_DEPOSIT_AMOUNT` and
`NEOFS_MORPH_NOTARY_DEPOSIT_DURATION` envs for Storage Node, that
have default values, not required. They should contain notary deposit
amount and frequency(in blocks) respectively.

All side chain contract address config values are optional. If side chain
contract address is not specified, then value gathered from NNS contract.

Added `NEOFS_STORAGE_SHARD_<N>_WRITECACHE_SIZE_LIMIT` where `<N>` is shard ID.
This is the size limit for the all write cache storages combined in bytes. Default
size limit is 1 GiB.

Added `NEOFS_STORAGE_SHARD_<N>_REFILL_METABASE` bool flag where `<N>` is shard
ID. This flag purges metabase instance at the application start and reinitialize
it with available objects from the blobstor.

Object service pool size now split into `NEOFS_OBJECT_PUT_POOL_SIZE_REMOTE` and
`NEOFS_OBJECT_PUT_POOL_SIZE_LOCAL` configuration records.

## [0.24.1] - 2021-09-07

### Fixed
- Storage and Inner Ring will not start until Neo RPC node will have the height
of the latest processed block by the nodes (#795)

### Updating from v0.24.0
Specify path to the local state DB in Inner Ring node config with
`NEOFS_IR_NODE_PERSISTENT_STATE_PATH`. Specify path to the local state DB in
Storage node config with `NEOFS_NODE_PERSISTENT_STATE_PATH`.

## [0.24.0] - 2021-08-30 Anmyeondo (안면도, 安眠島)

### Fixed
- Linter warning messages (#766)
- Storage Node does not register itself in network in relay mode now (#761)

### Changed
- `neofs-adm` fails when is called in a notary-disabled environment (#757)
- `neofs-adm` uses `neo-go` client's native NNS resolving method instead of the custom one (#756)
- Node selects pseudo-random list of objects from metabase for replication (#715)

### Added
- Contract update support in `neofs-adm` utility (#748)
- Container transferring support in `neofs-adm` utility (#755)
- Storage Node's balance refilling support in `neofs-adm` utility (#758)
- Support `COMMON_PREFIX` filter for object attributes in storage engine and `neofs-cli` (#760)
- Node's and IR's notary status debug message on startup (#758)
- Go `1.17` unit tests in CI (#766)
- Supporting all eACL filter fields from the specification (#768)
- Cache for Container service's read operations (#676)

### Updated
- `neofs-api-go` library to `v1.29.0`

### Removed
- Unused `DB_SIZE` parameter of writecache (#773)

### Updating from v0.23.1
Storage Node does not read unused `NEOFS_STORAGE_SHARD_XXX_WRITECACHE_DB_SIZE`
config parameter anymore.

## [0.23.1] - 2021-08-06

N3 Mainnet launch release with minor fixes.

### Added
- Initial version of `neofs-adm` tool for fast side chain deployment and
  management in private installations
- Notary support auto negotiation (#709)
- Option to disable side chain cache in Storage node (#704)
- Escape symbols in Storage node attributes (#700)

### Changed
- Updated neo-go to v0.97.1
- Updated multiaddr lib to v0.4.0 with native TLS protocol support (#661)
- Default file permission in storage engine is 660 (#646)

### Fixed
- Container size estimation routine now aggregates values by cid-epoch tuple
  (#723)
- Storage engine always creates executable dirs (#646)
- GC routines in storage engine shards shutdown gracefully (#745)
- Handle context shutdown at NeoFS multi client group address switching (#737)
- Scope for main chain invocations from Inner Ring nodes (#751)

### Updating from v0.23.0
Added `NEOFS_MORPH_DISABLE_CACHE` env. If `true`, none of
the `eACL`/`netmap`/`container` RPC responses cached.

Remove `WITHOUT_NOTARY` and `WITHOUT_MAIN_NOTARY` records from Inner Ring node
config. Notary support is now auto negotiated.

## [0.23.0] - 2021-07-23 - Wando (완도, 莞島)

Improved stability for notary disabled environment.

### Added
- Alphabet wallets generation command in neofs-adm (#684)
- Initial epoch timer tick synchronization at Inner Ring node startup (#679)

### Changed
- `--address` flag is optional in NeoFS CLI (#656)
- Notary subsystem now logs `ValidUntilBlock` (#677)
- Updated neo-go to v0.96.1
- Storage Node configuration example contains usable parameters (#699)

### Fixed
- Do not use side chain RoleManagement contract as source of Inner Ring list
  when notary disabled in side chain (#672)
- Alphabet list transition is even more effective (#697)
- Inner Ring node does not require proxy and processing contracts if notary
  disabled (#701, #714)

### Updating from v0.22.3
To upgrade Storage node or Inner Ring node from v0.22.3, you don't need to
change configuration files. Make sure, that NEO RPC nodes, specified in config,
are connected to N3 RC4 (Testnet) network.

## [0.22.3] - 2021-07-13

### Added
- Support binary eACL format in container CLI command ([#650](https://github.com/nspcc-dev/neofs-node/issues/650)).
- Dockerfile for neofs-adm utility ([#680](https://github.com/nspcc-dev/neofs-node/pull/680)).

### Changed
- All docker files moved to `.docker` dir ([#682](https://github.com/nspcc-dev/neofs-node/pull/682)).

### Fixed
- Do not require MainNet attributes in "Without MainNet" mode ([#663](https://github.com/nspcc-dev/neofs-node/issues/663)).
- Stable alphabet list merge in Inner Ring governance ([#670](https://github.com/nspcc-dev/neofs-node/issues/670)).
- User can specify only wallet section without node key ([#690](https://github.com/nspcc-dev/neofs-node/pull/690)).
- Log keys in hex format in reputation errors ([#693](https://github.com/nspcc-dev/neofs-node/pull/693)).
- Connections leak and reduced amount of connection overall ([#692](https://github.com/nspcc-dev/neofs-node/issues/692)).

### Removed
- Debug output of public key in Inner Ring log ([#689](https://github.com/nspcc-dev/neofs-node/pull/689)).

## [0.22.2] - 2021-07-07

Updated broken version of NeoFS API Go.

### Updated
- NeoFS API Go: [v1.28.3](https://github.com/nspcc-dev/neofs-api-go/releases/tag/v1.28.3).

## [0.22.1] - 2021-07-07

### Added
- `GetCandidates` method to morph client wrapper ([#647](https://github.com/nspcc-dev/neofs-node/pull/647)).
- All-in-One Docker image that contains all NeoFS related binaries ([#662](https://github.com/nspcc-dev/neofs-node/pull/662)).
- `--version` flag to Storage Node binary ([#664](https://github.com/nspcc-dev/neofs-node/issues/664)).

### Changed
- Do not check NeoFS version in `LocalNodeInfo` requests and `Put` container operations; `v2.7.0` is genesis version of NeoFS ([#660](https://github.com/nspcc-dev/neofs-node/pull/660)).
- All error calls of CLI return `1` exit code ([#657](https://github.com/nspcc-dev/neofs-node/issues/657)).

### Fixed
- Do not use multisignature for audit operations ([#658](https://github.com/nspcc-dev/neofs-node/pull/658)).
- Skip audit for containers without Storage Groups ([#659](https://github.com/nspcc-dev/neofs-node/issues/659)).

### Updated
- NeoFS API Go: [v1.28.2](https://github.com/nspcc-dev/neofs-api-go/releases/tag/v1.28.2).

## [0.22.0] - 2021-06-29 - Muuido (무의도, 舞衣島)

Storage nodes with a group of network endpoints.

### Added
- Support of Neo wallet credentials in CLI ([#610](https://github.com/nspcc-dev/neofs-node/issues/610)).
- More reliable approval of trust value by IR ([#500](https://github.com/nspcc-dev/neofs-node/issues/500)).
- Storage node's ability to announce and serve on multiple network addresses ([#607](https://github.com/nspcc-dev/neofs-node/issues/607)).
- Validation of network addresses of netmap candidates in IR ([#557](https://github.com/nspcc-dev/neofs-node/issues/557)).
- Control service with healthcheck RPC in IR and CLI support ([#414](https://github.com/nspcc-dev/neofs-node/issues/414)).

### Fixed
- Approval of objects with duplicate attribute keys or empty values ([#633](https://github.com/nspcc-dev/neofs-node/issues/633)).
- Approval of containers with duplicate attribute keys or empty values ([#634](https://github.com/nspcc-dev/neofs-node/issues/634)).
- Default path for CLI config ([#626](https://github.com/nspcc-dev/neofs-node/issues/626)).

### Changed
- `version` command replaced with `--version` flag in CLI ([#571](https://github.com/nspcc-dev/neofs-node/issues/571)).
- Command usage text is not printed on errors in CLI ([#623](https://github.com/nspcc-dev/neofs-node/issues/623)).
- `netmap snapshot` command replaced with `control netmap-snapshot` one in CLI ([#651](https://github.com/nspcc-dev/neofs-node/issues/651)).
- IR does not include nodes with LOCODE derived attributes to the network map ([#412](https://github.com/nspcc-dev/neofs-node/issues/412)).
- IR uses morph/client packages for contract invocations ([#496](https://github.com/nspcc-dev/neofs-node/issues/496)).
- Writecache decreases local size when objects are flushed ([#568](https://github.com/nspcc-dev/neofs-node/issues/568)).
- IR can override global configuration values only in debug build ([#363](https://github.com/nspcc-dev/neofs-node/issues/363)).

### Updated
- Neo Go: [v0.95.3](https://github.com/nspcc-dev/neo-go/releases/tag/v0.95.3).
- NeoFS API Go: [v1.28.0](https://github.com/nspcc-dev/neofs-api-go/releases/tag/v1.28.0).
- protobuf: [v1.26.0](https://github.com/protocolbuffers/protobuf-go/releases/tag/v1.26.0).
- uuid: [v1.2.0](https://github.com/google/uuid/releases/tag/v1.2.0).
- compress: [v1.13.1](https://github.com/klauspost/compress/releases/tag/v1.13.1).
- base58: [v1.2.0](https://github.com/mr-tron/base58/releases/tag/v1.2.0).
- multiaddr: [v0.3.2](https://github.com/multiformats/go-multiaddr/releases/tag/v0.3.2).
- ants: [v2.4.0](https://github.com/panjf2000/ants/releases/tag/v2.4.0).
- orb: [v0.2.2](https://github.com/paulmach/orb/releases/tag/v0.2.2).
- prometheus: [v1.11.0](https://github.com/prometheus/client_golang/releases/tag/v1.11.0).
- testify: [v1.7.0](https://github.com/stretchr/testify/releases/tag/v1.7.0).
- atomic: [v1.8.0](https://github.com/uber-go/atomic/releases/tag/v1.8.0).
- zap: [v1.17.0](https://github.com/uber-go/zap/releases/tag/v1.17.0).
- grpc: [v1.38.0](https://github.com/grpc/grpc-go/releases/tag/v1.38.0).
- cast: [v1.3.1](https://github.com/spf13/cast/releases/tag/v1.3.1).
- cobra: [1.1.3](https://github.com/spf13/cobra/releases/tag/v1.1.3).
- viper: [v1.8.1](https://github.com/spf13/viper/releases/tag/v1.8.1).

## [0.21.1] - 2021-06-10

### Fixed
- Session token lifetime check (#589).
- Payload size check on the relayed objects (#580).

### Added
- VMagent to collect metrics from testnet storage image

### Changed
- Updated neofs-api-go to v1.27.1 release.

## [0.21.0] - 2021-06-03 - Seongmodo (석모도, 席毛島)

Session token support in container service, refactored config in storage node,
TLS support on gRPC servers.

### Fixed
- ACL service traverses over all RequestMetaHeader chain to find
  bearer and session tokens (#548).
- Object service correctly resends complete objects without attached
  session token (#501).
- Inner ring processes `neofs.Bind` and `neofs.Unbind` notifications (#556).
- Client cache now gracefully closes all available connections (#567).

### Added
- Session token support in container service for `container.Put`,
  `container.Delete` and `container.SetEACL` operations.
- Session token support in container and sign command of NeoFS CLI.
- TLS encryption support of gRPC service in storage node.

### Changed
- Inner ring listens RoleManagement contract notifications to start governance
  update earlier.
- Inner ring processes extended ACL changes.
- Inner ring makes signature checks of containers and extended ACLs.
- Refactored config of storage node.
- Static clients from `morph/client` do not process notary invocations
  explicitly anymore. Now notary support specified at static client creation.
- Updated neo-go to v0.95.1 release.
- Updated neofs-api-go to v1.27.0 release.

### Removed
- Container policy parser moved to neofs-sdk-go repository.
- Invoke package from inner ring.

## [0.20.0] - 2021-05-21 - Dolsando (돌산도, 突山島)

NeoFS is N3 RC2 compatible.

### Fixed
- Calculations in EigenTrust algorithm (#527).
- NPE at object service request forwarding (#532, #543, #544).
- FSTree iterations in blobstor (#541).
- Inhume operation in storage engine (#546).

### Added
- Optional endpoint to main chain in storage app.
- Client for NeoFSID contract.

### Changed
- Reorganized and removed plenty of application configuration records
  (#510, #511, #512, #514).
- Nodes do not resolve remote addresses manually.
- Presets for basic ACL in CLI are `private` ,`public-read` and
  `public-read-write` now.
- Updated neo-go to v0.95.0 release.
- Updated neofs-api-go to v1.26.1 release.
- Updated go-multiaddr to v0.3.1 release.

### Removed
- Unused external GC workers (GC is part of the shard in storage engine now).
- Unused worker pools for object service in storage app.
- `pkg/errors` dependency (stdlib errors used instead).

## [0.19.0] - 2021-05-07 - Daecheongdo (대청도, 大靑島)

Storage nodes exchange, calculate, aggregate and store reputation information
in reputation contract. Inner ring nodes support workflows with and without
notary subsystem in chains.

### Fixed
- Build with go1.16.
- Notary deposits last more blocks.
- TX hashes now prints in little endian in logs.
- Metabase deletes graves regardless of the presence of objects.
- SplitInfo error created from all shards instead of first matched shard.
- Possible deadlock at cache eviction in blobovnicza.
- Storage node does not send rebootstrap messages after it went offline.

### Added
- Reputation subsystem that includes reputation collection, exchange,
calculation and storage components.
- Notary and non notary workflows in inner ring.
- Audit fee transfer for inner ring nodes that performed audit.
- Unified encoding for all side chain payment details.
- New write cache implementation for storage engine.
- NEP-2 and NEP-6 key formats in CLI.

### Changed
- Metabase puts data in batches.
- Network related new epoch handlers in storage node executed asynchronously.
- Storage node gets epoch duration from global config.
- Storage node resign and resend Search, Range, Head, Get requests of object
service without modification.
- Inner ring does not sync side chain validators in single chain deployment.
- neo-go updated to v0.94.1
- neofs-api-go updated to v1.26.0

## [0.18.0] - 2021-03-26 - Yeongheungdo (영흥도, 靈興島)

NeoFS operates with updated governance model. Alphabet keys and inner ring keys
are accessed from side chain committee and `RoleManagement` contract. Each epoch
alphabet keys are synchronized with main chain.

### Fixed
- Metabase does not store object payloads anymore.
- TTLNetCache now always evict data after a timeout.
- NeoFS CLI keyer could misinterpret hex value as base58.

### Added
- Local trust controller in storage node.
- Governance processor in inner ring that synchronizes list of alphabet keys.

### Changed
- Inner ring keys and alphabet keys are managed separately by inner ring and
  gathered from committee and `RoleManagement` contract.

## [0.17.0] - 2021-03-22 - Jebudo (제부도, 濟扶島)

Notary contract support, updated neofs-api-go with raw client, some performance
tweaks with extra caches and enhanced metrics.

### Added
- Notary contract support.
- Cache for morph client.
- Metrics for object service and storage engine.
- Makefile target for fast and dirty docker images.
- GAS threshold value in inner ring GAS transfers.

### Changed
- RPC client cache now re-used per address instead of (address+key) tuple.
- Updated neofs-api-go version to v1.25.0 with raw client support.
- Updated neo-go to testnet compatible v0.94.0 version.

## [0.16.0] - 2021-02-26 - Ganghwado (강화도, 江華島)

Garbage collector is now running inside storage engine. It is accessed
via Control API, from `policer` component and through object expiration
scrubbers.

Inner ring configuration now supports single chain mode with any number of
alphabet contracts.

Storage node now supports NetworkInfo method in netmap service.

### Fixed
- Storage engine now inhumes object only in single shard.
- Metabase correctly removes parent data at batched children delete.
- Metabase does not accept tombstone on tombstone records in graveyard anymore.
- Object service now rejects expired objects.
- CLI now correctly attaches bearer token in storage group operations.
- Container policy parser now works with strings in filter key.
- Policer component now removes redundant objects locally.

### Added
- GC job that monitors expired objects.
- GC job that removes marked objects from physical storage.
- Batch inhume operations in metabase, shard and engine.
- `control.DropObjects` RPC method.
- Support of `netmap.NetworkInfo` RPC method.
- Single chain inner ring configuration.

### Changed
- `UN-LOCODE` node attribute now optional.
- `engine.Delete` method now marks object to be removed by GC.
- Inner ring node supports any number of alphabet contracts from 1 up to 40.

## [0.15.0] - 2021-02-12 - Seonyudo (선유도, 仙遊島)

NeoFS nodes are now preview5-compatible.

IR nodes are now engaged in the distribution of funds to the storage nodes:
for the passed audit and for the amount of stored information. All timers
of the IR nodes related to the generation and processing of global system
events are decoupled from astronomical time, and are measured in the number
of blockchain blocks.

For the geographic positioning of storage nodes, a global NeoFS location
database is now used, the key in which is a UN/LOCODE, and the base itself
is generated on the basis of the UN/LOCODE and OpenFlights databases.

### Added
- Timers with time in blocks of the chain.
- Subscriptions to new blocks in blockchain event `Listener`.
- Tracking the volume of stored information by containers in the
  storage engine and an external interface for obtaining this data.
- `TransferX` operation in sidechain client.
- Calculators of audit and basic settlements.
- Distribution of funds to storage nodes for audit and for the amount
  of stored information (settlement processors of IR).
- NeoFS API `Container.AnnounceUsedSpace` RPC service.
- Exchange of information about container volumes between storage nodes
  controlled by IR through sidechain notifications.
- Support of new search matchers (`STRING_NOT_EQUAL`, `NOT_PRESENT`).
- Functional for the formation of NeoFS location database.
- CLI commands for generating and reading the location database.
- Checking the locode attribute and generating geographic attributes
  for candidates for a network map on IR side.
- Verification of the eACL signature when checking Object ACL rules.

### Fixed
- Overwriting the local configuration of node attributes when updating
  the network map.
- Ignoring the X-headers CLI `storagegroup` commands.
- Inability to attach bearer token in CLI `storagegroup` commands.

### Changed
- Units of epoch and emit IR intervals.
- Query language in CLI `object search` command.

### Updated
- neo-go v0.93.0.
- neofs-api-go v1.23.0.

## [0.14.3] - 2021-01-27

### Fixed
- Upload of objects bigger than single gRPC message.
- Inconsistent placement issues (#347, #349).
- Bug when ACL request classifier failed to classify `RoleOthers` in
  first epoch.

### Added
- Debug section in readme file for testnet configuration.

### Changed
- Docker images now based on alpine and contain shell.
- Node bootstraps with active state in node info structure.

## [0.14.2] - 2021-01-20

Testnet4 related bugfixes.

### Fixed
- Default values for blobovnicza object size limit and blobstor small object
  size are not zero.
- Various storage engine log messages.
- Bug when inner ring node ignored bootstrap messages from restarted storage
  nodes.

### Added
- Timeout for reading boltDB files at storage node initialization.

### Changed
- Increased default extra GAS fee for contract invocations at inner ring.

## [0.14.1] - 2021-01-15

### Fixed

- Inner ring node could not confirm `netmap.updateState` notification.
- `object.RangeHash` method ignored salt values.

### Added

- Control API service for storage node with health check, netmap and node state
  relate methods.
- Object service now looks to previous epoch containers.
- Possibility to configure up multiple NEO RPC endpoints in storage node.

### Changed

- Storage node shuts down if event producer RPC node is down.

## [0.14.0] - 2020-12-30 - Yeouido (여의도, 汝矣島)

Preview4 compatible NeoFS nodes with data audit.

### Added
- Data audit routines in inner ring nodes.
- Storage group operations in CLI (`neofs-cli storagegroup --help`).

### Fixed
- Loss of request X-headers during the forwarding in Object service.

### Changed
- Updated neo-go version for preview4 compatibility.

### Updated
- neo-go v0.92.0.
- neofs-api-go v1.22.0.

## [0.13.2] - 2020-12-24

Support changes from neofs-api-go v1.21.2 release.

### Added

- Support of request X-Headers in CLI commands.

### Changed

- Use updated API of container library.

## [0.13.1] - 2020-12-18

Fixes based on Modo release testing results.

### Added

- Verification of chain element addresses during object assembling.

### Changed

- Processing of filters by non-address fields in Object Range/RangeHash/Delete.

### Fixed

- `Graveyard` and `ToMoveIt` bucket names in metabase.
- Double formation of the parent title when transforming an object.
- Loss of session token during Object Put.
- Potential generating Range requests inside Get request execution context.

## [0.13.0] - 2020-12-15 - Modo (모도, 茅島)

Implementation of a local object storage engine.
Adaptation of the object service work scheme for the engine.

### Changed

- Object format after transformations.
- Handling of object operations.

### Added

- Local storage components: `Engine`, `Shard`, `BlobStor`,
  `Metabase`, `Blobovnicza`.
- Support of voting for sidechain governance in IR node.
- `Raw` flag support in Object Get/Head/GetRange CLI commands.

### Fixed

- Ignoring object address from session token in eACL validation.

## [0.12.1] - 2020-11-25

Bugfixes and small performance improvements.

### Fixed

- Routine leak by adding SDK client cache. (#184)
- Variety of ACL bugs. (#180, #190, #209)
- Policer tried to replicate virtual objects. (#182)
- Search queries with object ID field. (#177)
- Bug with extended ACL signature check in neofs-cli (#206)

### Added

- More debug logs in object service.
- Dial timeouts in object service config (`NEOFS_OBJECT_PUT_DIAL_TIMEOUT=5s`)

### Changed

- Routine pools in object service are non-blocking now.
- Container service now returns error if extended ACL is not set.

## [0.12.0] - 2020-11-17

NeoFS-API v2.0 support and updated brand-new storage node application.

### Fixed

- SetConfig method invocation of netmap contract. (#147)
- Balance response overflow. (#122)

### Added

- Gas emission routine in inner ring nodes.
- GRPC reflection service. (`NEOFS_GRPC_ENABLE_REFLECT_SERVICE=true`)
- New netmap query language parser.

### Changed

- Storage node application rebuilt from scratch.
- CLI supports accounting, object and container related operations.
- Inner ring node shutdowns on neo RPC node connection drop.
- Updated to preview4 compatible neo-go version.

## [0.11.0] - 2020-07-23

### Added

- Inner ring application to repository.
- Inner ring epoch processor.
- Inner ring asset processor for GAS deposit and withdraw.

### Changed

- The structure of source code tree.

## [0.10.0] - 2020-07-10

First public review release.

[Unreleased]: https://github.com/nspcc-dev/neofs-node/compare/v0.46.1...master
[0.46.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.46.0...v0.46.1
[0.46.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.45.2...v0.46.0
[0.45.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.45.1...v0.45.2
[0.45.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.45.0...v0.45.1
[0.45.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.44.2...v0.45.0
[0.44.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.44.1...v0.44.2
[0.44.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.44.0...v0.44.1
[0.44.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.43.0...v0.44.0
[0.43.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.42.1...v0.43.0
[0.42.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.42.0...v0.42.1
[0.42.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.41.1...v0.42.0
[0.41.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.41.0...v0.41.1
[0.41.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.40.1...v0.41.0
[0.40.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.40.0...v0.40.1
[0.40.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.39.2...v0.40.0
[0.39.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.39.1...v0.39.2
[0.39.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.39.0...v0.39.1
[0.39.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.38.1...v0.39.0
[0.38.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.38.0...v0.38.1
[0.38.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.37.0...v0.38.0
[0.37.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.36.1...v0.37.0
[0.36.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.36.0...v0.36.1
[0.36.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.35.0...v0.36.0
[0.35.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.34.0...v0.35.0
[0.34.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.33.0...v0.34.0
[0.33.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.32.0...v0.33.0
[0.32.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.31.0...v0.32.0
[0.31.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.30.2...v0.31.0
[0.30.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.30.1...v0.30.2
[0.30.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.30.0...v0.30.1
[0.30.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.29.0...v0.30.0
[0.29.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.28.3...v0.29.0
[0.28.3]: https://github.com/nspcc-dev/neofs-node/compare/v0.28.2...v0.28.3
[0.28.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.28.1...v0.28.2
[0.28.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.28.0...v0.28.1
[0.28.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.28.0-rc.3...v0.28.0
[0.28.0-rc.3]: https://github.com/nspcc-dev/neofs-node/compare/v0.28.0-rc.2...v0.28.0-rc.3
[0.28.0-rc.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.28.0-rc.1...v0.28.0-rc.2
[0.28.0-rc.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.7...v0.28.0-rc.1
[0.27.7]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.6...v0.27.7
[0.27.6]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.5...v0.27.6
[0.27.5]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.4...v0.27.5
[0.27.4]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.3...v0.27.4
[0.27.3]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.2...v0.27.3
[0.27.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.1...v0.27.2
[0.27.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.27.0...v0.27.1
[0.27.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.26.1...v0.27.0
[0.26.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.26.0...v0.26.1
[0.26.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.25.1...v0.26.0
[0.25.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.25.0...v0.25.1
[0.25.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.24.1...v0.25.0
[0.24.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.24.0...v0.24.1
[0.24.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.23.1...v0.24.0
[0.23.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.23.0...v0.23.1
[0.23.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.22.3...v0.23.0
[0.22.3]: https://github.com/nspcc-dev/neofs-node/compare/v0.22.2...v0.22.3
[0.22.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.22.1...v0.22.2
[0.22.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.22.0...v0.22.1
[0.22.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.21.1...v0.22.0
[0.21.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.21.0...v0.21.1
[0.21.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.20.0...v0.21.0
[0.20.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.19.0...v0.20.0
[0.19.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.18.0...v0.19.0
[0.18.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.14.3...v0.15.0
[0.14.3]: https://github.com/nspcc-dev/neofs-node/compare/v0.14.2...v0.14.3
[0.14.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.14.1...v0.14.2
[0.14.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.14.0...v0.14.1
[0.14.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.13.2...v0.14.0
[0.13.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.12.1...v0.13.0
[0.12.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.10.0...v0.11.0
