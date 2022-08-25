# Metabase versioning

This file describes changes between the metabase versions.

## Current

### Primary buckets
- Graveyard bucket
  - Name: `_Graveyard`
  - Key: object address 
  - Value: tombstone address
- Garbage bucket
  - Name: `_Garbage`
  - Key: object address
  - Value: dummy value
- Bucket containing IDs of objects that are candidates for moving
   to another shard.
  - Name: `_ToMoveIt`
  - Key: object address
  - Value: dummy value
- Container volume bucket
  - Name: `_ContainerSize`
  - Key: container ID
  - Value: container size in bytes as little-endian uint64
- Bucket for storing locked objects information
  - Name: `_Locked` 
  - Key: container ID
  - Value: bucket mapping objects locked to the list of corresponding LOCK objects
- Bucket containing auxilliary information. All keys are custom and are not connected to the container
  - Name: `_i`
  - Keys and values
    - `id` -> shard id as bytes
    - `version` -> metabase version as little-endian uint64
    - `phy_counter` -> shard's physical object counter as little-endian uint64

### Unique index buckets
- Buckets containing objects of REGULAR type
  - Name: container ID
  - Key: object ID
  - Value: marshalled object
- Buckets containing objects of LOCK type
  - Name: container ID + `_LOCKER`
  - Key: object ID
  - Value: marshalled object
- Buckets containing objects of STORAGEGROUP type
  - Name: container ID + `_SG`
  - Key: object ID
  - Value: marshaled object
- Buckets containing objects of TOMBSTONE type
  - Name: container ID + `_TS`
  - Key: object ID
  - Value: marshaled object
- Buckets mapping objects to the storage ID they are stored in
  - Name: container ID + `_small`
  - Key: object ID
  - Value: storage ID
- Buckets for mapping parent object to the split info
  - Name: container ID + `_root`
  - Key: object ID
  - Value: split info

### FKBT index buckets
- Buckets mapping owner to object IDs
  - Name: containerID + `_ownerid`
  - Key: owner ID as base58 string
  - Value: bucket containing object IDs as keys
- Buckets containing objects attributes indexes
  - Name: containerID + `_attr_` + attribute key
  - Key: attribute value
  - Value: bucket containing object IDs as keys

### List index buckets
- Buckets mapping payload hash to a list of object IDs
  - Name: container ID + `_payloadhash`
  - Key: payload hash
  - Value: list of object IDs
- Buckets mapping parent ID to a list of children IDs
  - Name: container ID + `_parent`
  - Key: parent ID
  - Value: list of children object IDs
- Buckets mapping split ID to a list of object IDs
  - Name: container ID + `_splitid`
  - Key: split ID
  - Value: list of object IDs


## Version 1

- Metabase now stores generic storage id instead of blobovnicza ID.

## Version 0

- Container ID is encoded as base58 string
- Object ID is encoded as base58 string
- Address is encoded as container ID + "/" + object ID