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
- Bucket containing IDs of objects that are candidates for moving
   to another shard.
  - Name: `2`
  - Key: object address
  - Value: dummy value
- Container volume bucket
  - Name: `3`
  - Key: container ID
  - Value: bucket with container metrics:
            - `0` -> container size in bytes as little-endian uint64
            - `1` -> container's objects number as little-endian uint64
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
- Metadata bucket
  - Name: `255` + container ID
  - Keys without values
    - `0` + object ID
    - `1` + attribute + `0x00` + `0|1` + fixed256(value) + object ID: integer attributes. \
      Sign byte is 0 for negatives, 1 otherwise. Bits are inverted for negatives also.
    - `2` + attribute + `0x00` + value + `0x00` + object ID
    - `3` + object ID + attribute + `0x00` + value
    - `4` — container-level GC mark. \
      Presence means the whole container is scheduled for garbage collection.

# History

## Version 8

Container statistic is now a bucket with multiple values. In the version number
of objects was added

Drop garbage containers index (17), replaced with mark in metadata bucket.

## Version 7

Fixed version 6 which could store OID keys in garbage object bucket.

## Version 6

Dropped the following buckets:
 * regular object index (6)
 * lock object index (7)
 * storage group object index (8)
 * tombstone object index (9)
 * link object index (18)

## Version 5

Dropped the following buckets:
 * small object (10)
 * owner index (12)
 * user attribute index (13)
 * payload hashes (14)
 * split ID index (16)
 * first object ID index (19)
 * parent ID to children index (15)
 * parent ID to split info (11)

## Version 4

Introduced metadata bucket (255) for SearchV2 API and other purposes.

## Version 3

Changed graveyard to store expiration epoch along with tombstone address.

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
