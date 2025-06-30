# NeoFS Admin Tool

## Overview

Admin tool provides an easier way to deploy and maintain private installation
of NeoFS. Private installation provides a set of N3 consensus nodes, NeoFS
Alphabet, and Storage nodes. Admin tool generates consensus keys, initializes
the sidechain, and provides functions to update the network and register new
Storage nodes.

## Build

To build binary locally, use `make bin/neofs-adm` command.

For clean build inside a docker container, use `make docker/bin/neofs-adm`.

Build docker image with `make image-adm`.

At NeoFS private install deployment, neofs-adm requires compiled NeoFS
contracts. Find them in the latest release of
[neofs-contract repository](https://github.com/nspcc-dev/neofs-contract/releases).


## Commands

### Config

Config section provides `init` command that creates a configuration file for
private installation deployment and updates. Config file is optional, all
parameters can be passed by arguments or read from standard input (wallet
passwords).

Config example:
```yaml
rpc-endpoint: https://address:port # sidechain RPC node endpoint
alphabet-wallets: /path            # path to consensus node / alphabet wallets storage
network:
  max_object_size: 67108864 # max size of a single NeoFS object, bytes
  epoch_duration: 240       # duration of a NeoFS epoch in blocks, consider block generation frequency in the sidechain
  basic_income_rate: 0      # basic income rate, for private consider 0
  fee:
    audit: 0     # network audit fee, for private installation consider 0
    candidate: 0 # inner ring candidate registration fee, for private installation consider 0
    container: 0 # container creation fee, for private installation consider 0
    container_alias: 0 # container nice-name registration fee, for private installation consider 0
    withdraw: 0  # withdraw fee, for private installation consider 0
credentials:     # passwords for consensus node / alphabet wallets
  az: password1
  buky: password2
  vedi: password3
  glagoli: password4
  dobro: password5
  yest: password6
  zhivete: password7
```

### FS chain

#### Network deployment

- `generate-alphabet` generates a set of wallets for consensus and
  Alphabet nodes.

- `init` initializes the sidechain by deploying smart contracts and
  setting provided NeoFS network configuration.

- `generate-storage-wallet` generates a wallet for the Storage node that
  is ready for deployment. It also transfers a bit of sidechain GAS, so this
  wallet can be used for NeoFS bootstrap.

#### Network maintenance

- `dump-names` allows to walk through NNS names and see their expirations.

- `set-config` add/update configuration values in the Netmap contract.

- `force-new-epoch` increments NeoFS epoch number and executes new epoch
  handlers in NeoFS nodes.

- `renew-domain` updates expiration date of the given domain for one year.

- `refill-gas` transfers sidechain GAS to the specified wallet.

- `update-contracts` updates contracts to a new version.

#### Container migration

If a network has to be redeployed, these commands will migrate all container meta
info. These commands **do not migrate actual objects**.

- `dump-containers` saves all containers and metadata registered in the container
  contract to a file.

- `restore-containers` restores previously saved containers by their repeated registration in
 the container contract.

- `list-containers` output all containers ids.

#### Network info

- `dump-config` prints NeoFS network configuration.

- `dump-hashes` prints NeoFS contract addresses stored in NNS.


## Private network deployment

Read step-by-step guide of private storage deployment [in docs](./deploy.md).
