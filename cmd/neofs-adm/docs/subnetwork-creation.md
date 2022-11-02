# NeoFS subnetwork creation

This is a short guide on how to create NeoFS subnetworks. This guide
considers that the sidechain and the inner ring (alphabet nodes) have already been
deployed and the sidechain contains a deployed `subnet` contract.

## Prerequisites

To follow this guide, you need:
- neo-go sidechain RPC endpoint;
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

**NOTE:** in notary-enabled environment you should have a sufficient
notary deposit (not expired, with enough GAS balance). Your subnet ID
will differ from the example.

The default account in the wallet that has been passed with `-w` flag is the owner
of the just created subnetwork.

You can check if your subnetwork was created successfully:

```shell
$ neofs-adm morph subnet get \
    -r <side_chain_RPC_endpoint> \
    --subnet <subnet_ID>
Owner: NUc734PMJXiqa2J9jRtvskU3kCdyyuSN8Q
```
Your owner will differ from the example.
