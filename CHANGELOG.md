# Changelog
Changelog for NeoFS Node

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
