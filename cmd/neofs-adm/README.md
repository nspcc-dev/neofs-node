# NeoFS Admin Tool

## Overview

Admin tool provides an easier way to deploy and maintain private installation
of NeoFS. Private installation contains a set of consensus N3 nodes, NeoFS 
Alphabet, and Storage nodes. Admin tool generates consensus keys, initializes 
side chain and provides functions to update the network and register new
Storage nodes.

## Build

To build binary locally use `make bin/neofs-adm` command. 

For clean build inside docker container use `make docker/bin/neofs-adm`. 

Build docker image with `make image-adm`.

At NeoFS private install deployment, neofs-adm requires compiled NeoFS 
contracts. Find them in the latest release of 
[neofs-contract repository](https://github.com/nspcc-dev/neofs-contract/releases).


## Commands

### Config

Config section provides `init` command that creates configuration file for the
private installation deployment and updates. Config file is optional, all
parameters may be passed by arguments or read from standard input (wallet 
passwords).

Config example:
```yaml
rpc-endpoint: https://address:port # side chain RPC node endpoint
alphabet-wallets: /path            # path to consensus node / alphabet wallets storage
network:
  max_object_size: 67108864 # max size of single NeoFS object, bytes
  epoch_duration: 240       # duration of NeoFS epoch in blocks, consider block generation frequency in side chain
  basic_income_rate: 0      # basic income rate, for private consider 0
  fee:
    audit: 0     # network audit fee, for private installation consider 0
    candidate: 0 # inner ring candidate registration fee, for private installation consider 0
    container: 0 # container creation fee, for private installation consider 0
    withdraw: 0  # withdraw fee, for private installation consider 0
credentials:     # passwords for consensus node / alphabet wallets
  az: ...
  buky: ...
  vedi: ...
  glagoli: ...
  dobro: ...
  yest: ...
  zhivete: ...
```

### Morph

#### Network deployment

- Use `generate-alphabet` to generate set of wallets for consensus and 
  Alphabet nodes. 

- `init` will initialize side chain by deploying smart contracts and
  setting provided NeoFS network configuration.

- `generate-storage-wallet` generates wallet for the Storage node that 
  is ready for deployment. It also transfers a bit of side chain GAS so this 
  wallet can be used for NeoFS bootstrap.

#### Network maintenance

- `force-new-epoch` increments NeoFS epoch number and enforces new epoch
  handlers in NeoFS nodes.

- `refill-gas` transfers side chain GAS to specified wallet. 

- `update-contracts` starts contract update routine.

#### Container migration

If network has to be redeployed, these commands will migrate all container meta
info. These commands **do not migrate actual objects**.

- `dump-containers` saves all containers and metadata registered in container
  contract to a file.

- `restore-containers` restores previously saved containers in container
  contract.

#### Network info

- `dump-config` prints NeoFS network configuration.

- `dump-hashes` prints NeoFS contract addresses stored in NNS.


## Private network deployment

Read step-by-step guide of private storage deployment [in docs](./docs/deploy.md).
