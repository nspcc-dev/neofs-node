# Managing Subnetworks

This is a short guide on how to manage NeoFS subnetworks. This guide
considers that side chain and inner ring (alphabet nodes) have already
been deployed and side chain contains deployed `subnet` contract.

## Prerequisites

- neo-go side chain RPC endpoint;
- latest released version of [neofs-adm](https://github.com/nspcc-dev/neofs-node/releases);
- [created](subnetwork-creation.md) subnetwork;
- wallet with account that owns the subnetwork;
- public key of the Storage Node;
- public keys of the node and client administrators;
- owner IDs of the NeoFS users.

## Add node administrator

Node administrators are the accounts that can manage (add and delete nodes)
whitelist of the nodes that could be included to the subnetwork. Only subnet
owner is allowed to add and remove node administrators from subnetwork.

```shell
$ neofs-adm morph subnet admin add \
    -r <side_chain_RPC_endpoint> \
    -w </path/to/owner/wallet> \
    --admin <HEX_admin_public_key> \
    --subnet <subnet_ID>
Add admin request sent successfully.
```

## Add node

Adding a node to a subnetwork means that the node becomes able to service
containers that have been created in that subnetwork. Addition only changes
list of the allowed nodes. Node is not required to be bootstrapped at the
moment of its inclusion.

```shell
$ neofs-adm morph subnet node add \
    -r <side_chain_RPC_endpoint> \
    -w </path/to/node_admin/wallet> \
    --node <HEX_node_public_key> \
    --subnet <subnet_ID>
Add node request sent successfully.
```

**NOTE:** owner of the subnetwork is also allowed to add nodes.

## Add client administrator

Client administrators are the accounts that can manage (add and delete
nodes) whitelist of the clients that can creates containers in the
subnetwork. Only subnet owner is allowed to add and remove client
administrators from subnetwork.

```shell
$ neofs-adm morph subnet admin add \
    -r <side_chain_RPC_endpoint> \
    -w </path/to/owner/wallet> \
    --admin <HEX_admin_public_key> \
    --subnet <subnet_ID> \
    --client \
    --group <group_ID>
Add admin request sent successfully.
```

**NOTE:** you do not need to create group explicitly, it would be created
right after the first client admin has been added. Group ID is 4-byte
positive integer number.

## Add client

```shell
$ neofs-adm morph subnet client add \
    -r <side_chain_RPC_endpoint> \
    -w </path/to/client_admin/wallet> \
    --client <client_ownerID> \
    --subnet <subnet_ID> \
    --group <group_ID>
Add client request sent successfully.
```

**NOTE:** owner of the subnetwork is also allowed to add clients. This is
the only one command that accepts `ownerID`, not the public key.
Administrator can manage only his group (a group where that administrator
has been added by subnet owner).

# Bootstrapping Storage Node

After subnetwork [creation](subnetwork-creation.md) and inclusion node to it, the 
node could be bootstrapped and service subnetwork containers.

For bootstrapping you need to specify ID of the subnetwork in the node's 
configuration: 

```yaml
...
node:
  ...
  subnet:
    entries: # list of IDs of subnets to enter in a text format of NeoFS API protocol (overrides corresponding attributes)
      - <subnetwork_ID>
  ...
...
```

**NOTE:** specifying subnetwork that is denied for the node is not an error:
that configuration value would be ignored. You do not need to specify zero 
(with 0 ID) subnetwork: its inclusion is implicit. On the contrary, to exclude
node from the default zero subnetwork you need to specify it explicitly:

```yaml
...
node:
  ...
  subnet:
    exit_zero: true # toggle entrance to zero subnet (overrides corresponding attribute and occurrence in `entries`)
  ...
...
```

# Creating container in non-zero subnetwork

Creating containers without using `--subnet` flag is equivalent to the 
creating container in the zero subnetwork.

To create container in a private network your wallet must have been added to
the client whitelist by client admins or subnet owners:

```shell
$ neofs-cli container create \
    --policy 'REP 1' \
    -w </path/to/wallet> \
    -r s01.neofs.devenv:8080 \
    --subnet <subnet_ID>
```
