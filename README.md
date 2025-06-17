<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./.github/logo_dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="./.github/logo_light.svg">
    <img src="./.github/logo_light.svg"  width="500px" alt="NeoFS logo">
  </picture>
</p>
<p align="center">
  <a href="https://fs.neo.org">NeoFS</a> is a decentralized distributed object storage integrated with the <a href="https://neo.org">NEO Blockchain</a>.
</p>

---
[![codecov](https://codecov.io/gh/nspcc-dev/neofs-node/branch/master/graph/badge.svg)](https://codecov.io/gh/nspcc-dev/neofs-node)
[![Report](https://goreportcard.com/badge/github.com/nspcc-dev/neofs-node)](https://goreportcard.com/report/github.com/nspcc-dev/neofs-node)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/nspcc-dev/neofs-node?sort=semver)
![License](https://img.shields.io/github/license/nspcc-dev/neofs-node.svg?style=popout)

# Overview

NeoFS nodes are organized in a peer-to-peer network that takes care of storing
and distributing user's data. Any Neo user may participate in the network and
get paid for providing storage resources to other users or store their data in
NeoFS and pay a competitive price for it.

Users can reliably store object data in the NeoFS network and have a transparent
data placement process due to a decentralized architecture and flexible storage
policies. Each node is responsible for executing the storage policies that the
users select for geographical location, reliability level, number of nodes, type
of disks, capacity, etc. Thus, NeoFS gives full control over data to users.

Deep [Neo Blockchain](https://neo.org) integration allows NeoFS to be used by
dApps directly from
[NeoVM](https://docs.neo.org/docs/en-us/basic/technology/neovm.html) on the
[Smart Contract](https://docs.neo.org/docs/en-us/intro/glossary.html)
code level. This way dApps are not limited to on-chain storage and can
manipulate large amounts of data without paying a prohibitive price.

NeoFS has a native [gRPC API](https://github.com/nspcc-dev/neofs-api) and has
protocol gateways for popular protocols such as [AWS
S3](https://github.com/nspcc-dev/neofs-s3-gw),
[HTTP](https://github.com/nspcc-dev/neofs-http-gw),
[FUSE](https://wikipedia.org/wiki/Filesystem_in_Userspace) and
[sFTP](https://en.wikipedia.org/wiki/SSH_File_Transfer_Protocol) allowing
developers to integrate applications without rewriting their code.

# Supported platforms

Now, we only support GNU/Linux on amd64 CPUs with AVX/AVX2 instructions. More
platforms will be officially supported after release `1.0`.

The latest version of neofs-node works with neofs-contract
[v0.19.1](https://github.com/nspcc-dev/neofs-contract/releases/tag/v0.19.1).

# Building

To make all binaries you need Go 1.23+ and `make`:
```
make all
```
The resulting binaries will appear in `bin/` folder.

To make a specific binary use:
```
make bin/neofs-<name>
```
See the list of all available commands in the `cmd` folder.

## Building with Docker

Building can also be performed in a container:
```
make docker/all                     # build all binaries
make docker/bin/neofs-<name> # build a specific binary
```

## Docker images

To make docker images suitable for use in [neofs-dev-env](https://github.com/nspcc-dev/neofs-dev-env/) use:
```
make images
```

# Running

## CLI

`neofs-cli` allows to perform a lot of actions like container/object management
connecting to any node of the target network. It has an extensive description
for all of its commands and options internally, but some specific concepts
have additional documents describing them:
 * [Sessions](docs/cli-sessions.md)
 * [Extended headers](docs/cli-xheaders.md)
 * [Exit codes](docs/cli-exit-codes.md)

See [docs/cli-commands](docs/cli-commands) for information about all cli commands.

`neofs-adm` is a network setup and management utility usually used by the
network administrators. Refer to [docs/cli-adm.md](docs/cli-adm.md) for mode
information about it.

Both neofs-cli and neofs-adm can take configuration file as a parameter to
simplify working with the same network/wallet. See
[cli.yaml](config/example/cli.yaml) for an example of what this config may look
like. Control service-specific configuration examples are
[ir-control.yaml](config/example/ir-control.yaml) and
[node-control.yaml](config/example/node-control.yaml) for IR and SN nodes
respectively.

## Node

There are two kinds of nodes -- inner ring nodes and storage nodes. Most of
the time you're interested in running a storage node, because inner ring ones
are special and are somewhat similar to Neo consensus nodes in their role for
the network. Both accept parameters from YAML or JSON configuration files and
environment variables.

See [docs/sighup.md](docs/sighup.md) on how nodes can be reconfigured without
restart.

See [docs/storage-node-configuration.md](docs/storage-node-configuration.md)
on how to configure a storage node.

### Example configurations

These examples contain all possible configurations of NeoFS nodes. All
parameters are correct there, however, their particular values are provided
for informational purposes only (and not recommended for direct use), real
networks and real configuration are likely to differ a lot for them.

 See [node.yaml](node.yaml) for configuration notes.
- Storage node
  - YAML (with comments): [node.yaml](config/example/node.yaml)
  - JSON: [node.json](config/example/node.json)
  - Environment: [node.env](config/example/node.env)
- Inner ring
  - YAML: [ir.yaml](config/example/ir.yaml)
  - Environment: [ir.env](config/example/ir.env)

# Private network

If you're planning on NeoFS development take a look at
[neofs-dev-env](https://github.com/nspcc-dev/neofs-dev-env/). To develop
applications using NeoFS we recommend more light-weight
[neofs-aio](https://github.com/nspcc-dev/neofs-aio) container. If you really
want to get your hands dirty refer to [docs/deploy.md](docs/deploy.md) for
instructions on how to do things manually from scratch.

# Contributing

Feel free to contribute to this project after reading the [contributing
guidelines](CONTRIBUTING.md).

Before starting to work on a certain topic, create a new issue first, describing
the feature/topic you are going to implement.

# Credits

NeoFS is maintained by [NeoSPCC](https://nspcc.ru) with the help and
contributions from community members.

Please see [CREDITS](CREDITS.md) for details.

# License

- [GNU General Public License v3.0](LICENSE)
