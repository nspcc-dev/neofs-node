# Changelog
Changelog for NeoFS Node

## [0.14.0] - 2020-12-XX Yeouido (여의도, 汝矣島)

Preview4 compatible NeoFS nodes with data audit.

### Added
- Data audit routines in inner ring nodes.
- Storage group operations in CLI (`neofs-cli storagegroup --help`)

### Changed
- Updated neo-go version for preview4 compatibility

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

[0.14.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.13.2...v0.14.0
[0.13.2]: https://github.com/nspcc-dev/neofs-node/compare/v0.13.1...v0.13.2
[0.13.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.13.0...v0.13.1
[0.13.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.12.1...v0.13.0
[0.12.1]: https://github.com/nspcc-dev/neofs-node/compare/v0.12.0...v0.12.1
[0.12.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/nspcc-dev/neofs-node/compare/v0.10.0...v0.11.0