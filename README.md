<p align="center">
<img src="./.github/logo.svg" width="500px" alt="NeoFS">
</p>
<p align="center">
  <a href="https://fs.neo.org">NeoFS</a> is a decentralized distributed object storage integrated with the <a href="https://neo.org">NEO Blockchain</a>.
</p>

---
[![Report](https://goreportcard.com/badge/github.com/nspcc-dev/neo-go)](https://goreportcard.com/report/github.com/nspcc-dev/neofs-node)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/nspcc-dev/neofs-node?sort=semver)
![License](https://img.shields.io/github/license/nspcc-dev/neofs-node.svg?style=popout)

# Overview

NeoFS Nodes are organized in peer-to-peer network that takes care of storing and
distributing user's data. Any Neo user may participate in the network and get
paid for providing storage resources to other users or store his data in NeoFS
and pay a competitive price for it.

Users can reliably store object data in the NeoFS network and have a transparent
data placement process due to decentralized architecture and flexible storage
policies. Each node is responsible for executing the storage policies that the
users select for geographical location, reliability level, number of nodes, type
of disks, capacity, etc. Thus, NeoFS gives full control over data to users.

Deep [Neo Blockchain](https://neo.org) integration allows NeoFS to be used by
dApp directly from
[NeoVM](https://docs.neo.org/docs/en-us/basic/technology/neovm.html) on the
[Smart Contract](https://docs.neo.org/docs/en-us/basic/technology/neocontract.html)
code level. This way dApps are not limited to on-chain storage and can
manipulate large amounts of data without paying a prohibitive price.

NeoFS has native [gRPC](https://grpc.io) API and popular protocol gates such as
[AWS S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html),
[HTTP](https://wikipedia.org/wiki/Hypertext_Transfer_Protocol),
[FUSE](https://wikipedia.org/wiki/Filesystem_in_Userspace) and
[sFTP](https://en.wikipedia.org/wiki/SSH_File_Transfer_Protocol) allowing
developers to easily integrate applications without rewriting their code.

# Contributing

Feel free to contribute to this project after reading the [contributing
guidelines](CONTRIBUTING.md).

Before starting to work on a certain topic, create an new issue first,
describing the feature/topic you are going to implement.

# Credits

NeoFS is maintained by [NeoSPCC](https://nspcc.ru) with the help and
contributions from community members.

Please see [CREDITS](CREDITS.md) for details.

# License

- [GNU General Public License v3.0](LICENSE)
