# Changelog
Changelog for NeoFS Node

## [Unreleased]

### Added
- Support binary eACL format in container CLI command ([#650](https://github.com/nspcc-dev/neofs-node/issues/650)).
- Dockerfile for neofs-adm utility ([#680](https://github.com/nspcc-dev/neofs-node/pull/680)).

### Changed
- All docker files moved to `.docker` dir ([#682](https://github.com/nspcc-dev/neofs-node/pull/682)).

### Fixed
- Do not require MainNet attributes in "Without MainNet" mode ([#663](https://github.com/nspcc-dev/neofs-node/issues/663)).
- Stable alphabet list merge in Inner Ring governance ([#670](https://github.com/nspcc-dev/neofs-node/issues/670)).
- User can specify only wallet section without node key ([#690](https://github.com/nspcc-dev/neofs-node/pull/690)).

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
- Approval of objects with with duplicate attribute keys or empty values ([#633](https://github.com/nspcc-dev/neofs-node/issues/633)). 
- Approval of containers with with duplicate attribute keys or empty values ([#634](https://github.com/nspcc-dev/neofs-node/issues/634)).
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

[Unreleased]: https://github.com/nspcc-dev/neofs-node/compare/v0.22.2...master
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
