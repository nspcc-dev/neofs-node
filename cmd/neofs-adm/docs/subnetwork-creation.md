# NeoFS subnetwork creation

This is a short guide on how to create NeoFS subnetworks. This guide 
considers that side chain and inner ring (alphabet nodes) have already 
been deployed and side chain contains deployed `subnet` contract.

## Prerequisites

To follow this guide you need:
- neo-go side chain RPC endpoint;
- latest released version of [neofs-adm](https://github.com/nspcc-dev/neofs-node/releases);
- wallet with NeoFS account.

## Creation

```shell
$ neofs-adm morph subnet create \
    -r <side_chain_RPC_endpoint> \
    -w </path/to/owner/wallet> \
    --notary
Create subnet request sent successfully. ID: 4223489767.
```

**NOTE:** use `--notary` only in notary-enabled environmental. You need to
have sufficient notary deposit (not expired with enough GAS balance). This
is the only one command that requires alphabet signature and, therefore,
the only one command that needs `--notary` flag. Your subnet ID will differ
from the example.

Default account in the wallet that was passed with `-w` flag is the owner
of the just created subnetwork.

You can check if your subnetwork was created successfully:

```shell
$ neofs-adm morph subnet get \
    -r <side_chain_RPC_endpoint> \
    --subnet <subnet_ID>
Owner: NUc734PMJXiqa2J9jRtvskU3kCdyyuSN8Q
```
Your owner will differ from the example.
