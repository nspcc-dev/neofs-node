# Metabase versioning

This file describes changes between the metabase versions.

## Current

Numbers stand for a single byte value unless otherwise stated.
The lowest not used bucket index: 20.

### Primary buckets
- Graveyard bucket
  - Name: `0`
  - Key: object address 
  - Value: tombstone address + little-endian uint64 tombstone expiration epoch
- Garbage objects bucket
  - Name: `1`
  - Key: object address
  - Value: dummy value
- Garbage containers bucket
  - Name: `17`
  - Key: container ID
  - Value: dummy value
- Bucket containing IDs of objects that are candidates for moving
   to another shard.
  - Name: `2`
  - Key: object address
  - Value: dummy value
- Container volume bucket
  - Name: `3`
  - Key: container ID
  - Value: container size in bytes as little-endian uint64
- Bucket for storing locked objects information
  - Name: `4` 
  - Key: container ID
  - Value: bucket mapping objects locked to the list of corresponding LOCK objects
- Bucket containing auxiliary information. All keys are custom and are not connected to the container
  - Name: `5`
  - Keys and values
    - `id` -> shard id as bytes
    - `version` -> metabase version as little-endian uint64
    - `phy_counter` -> shard's physical object counter as little-endian uint64
    - `logic_counter` -> shard's logical object counter as little-endian uint64
    - `last_resync_epoch` -> last epoch when metabase was resynchronized as little-endian uint64

### Unique index buckets
- Bucket containing objects of REGULAR type
  - Name: container ID
  - Key: object ID
  - Value: marshalled object
- Bucket containing objects of LOCK type
  - Name: `7` + container ID
  - Key: object ID
  - Value: marshalled object
- Bucket containing objects of STORAGEGROUP type
  - Name: `8` + container ID
  - Key: object ID
  - Value: marshaled object
- Bucket containing objects of TOMBSTONE type
  - Name: `9` + container ID
  - Key: object ID
  - Value: marshaled object
- Bucket containing object or LINK type
  - Name: `18` + container ID
  - Key: object ID
  - Value: marshaled object
- Bucket mapping objects to the storage ID they are stored in
  - Name: `10` + container ID
  - Key: object ID
  - Value: storage ID
- Bucket for mapping parent object to the split info
  - Name: `11` + container ID
  - Key: object ID
  - Value: split info

### FKBT index buckets
- Bucket mapping owner to object IDs
  - Name: `12` + container ID
  - Key: owner ID as base58 string
  - Value: bucket containing object IDs as keys
- Bucket containing objects attributes indexes
  - Name: `13` + container ID + attribute key
  - Key: attribute value
  - Value: bucket containing object IDs as keys

### List index buckets
- Bucket mapping payload hash to a list of object IDs
  - Name: `14` + container ID
  - Key: payload hash
  - Value: list of object IDs
- Bucket mapping parent ID to a list of children IDs
  - Name: `15` + container ID
  - Key: parent ID
  - Value: list of children object IDs
- Bucket mapping split ID to a list of object IDs
  - Name: `16` + container ID
  - Key: split ID
  - Value: list of object IDs
- Bucket mapping first object ID to a list of objects IDs
  - Name: `19` + container ID
  - Key: first object ID
  - Value: objects for corresponding split chain
- Metadata bucket
  - Name: `255` + container ID
  - Keys without values
    - `0` + object ID
    - `1` + attribute + `0x00` + `0|1` + fixed256(value) + object ID: integer attributes. \
      Sign byte is 0 for negatives, 1 otherwise. Bits are inverted for negatives also.
    - `2` + attribute + `0x00` + value + `0x00` + object ID
    - `3` + object ID + attribute + `0x00` + value

# History

## Version 3

Last version without metadata bucket introduced with `ObjectService.SearchV2` API.

## Version 2

- Container ID is encoded as 32-byte slice
- Object ID is encoded as 32-byte slice
- Object ID is encoded as 64-byte slice, container ID + object ID
- Bucket naming scheme is changed:
  - container ID + suffix -> 1-byte prefix + container ID

## Version 1

- Metabase now stores generic storage id instead of blobovnicza ID.

## Version 0

- Container ID is encoded as base58 string
- Object ID is encoded as base58 string
- Address is encoded as container ID + "/" + object ID
