# Managing Subnetworks

This is a short guide on how to manage NeoFS subnetworks. This guide
considers that the sidechain and the inner ring (alphabet nodes) have already been
deployed, and the sidechain contains a deployed `subnet` contract.

## Prerequisites

- neo-go sidechain RPC endpoint;
- latest released version of [neofs-adm](https://github.com/nspcc-dev/neofs-node/releases);
- [created](subnetwork-creation.md) subnetwork;
- wallet with the account that owns the subnetwork;
- public key of the Storage Node;
- public keys of the node and client administrators;
- owner IDs of the NeoFS users.

## Add node administrator

Node administrators are accounts that can manage (add and delete nodes)
the whitelist of the nodes which can be included to a subnetwork. Only the subnet
owner is allowed to add and remove node administrators from the subnetwork.

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
the list of the allowed nodes. Node is not required to be bootstrapped at the
moment of its inclusion.

```shell
$ neofs-adm morph subnet node add \
    -r <side_chain_RPC_endpoint> \
    -w </path/to/node_admin/wallet> \
    --node <HEX_node_public_key> \
    --subnet <subnet_ID>
Add node request sent successfully.
```

**NOTE:** the owner of the subnetwork is also allowed to add nodes.

## Add client administrator

Client administrators are accounts that can manage (add and delete
nodes) the whitelist of the clients that can create containers in the
subnetwork. Only the subnet owner is allowed to add and remove client
administrators from the subnetwork.

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

**NOTE:** you do not need to create a group explicitly, it will be created
right after the first client admin is added. Group ID is a 4-byte
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

**NOTE:** the owner of the subnetwork is also allowed to add clients. This is
the only one command that accepts `ownerID`, not the public key.
Administrator can manage only their group (a group where that administrator
has been added by the subnet owner).

# Bootstrapping Storage Node

After a subnetwork [is created](subnetwork-creation.md) and a node is included into it, the
node could be bootstrapped and service subnetwork containers.

For bootstrapping, you need to specify the ID of the subnetwork in the node's
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
a node from the default zero subnetwork, you need to specify it explicitly:

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

Creating containers without using `--subnet` flag is equivalent to
creating container in the zero subnetwork.

To create a container in a private network, your wallet must be added to
the client whitelist by the client admins or the subnet owners:

```shell
$ neofs-cli container create \
    --policy 'REP 1' \
    -w </path/to/wallet> \
    -r s01.neofs.devenv:8080 \
    --subnet <subnet_ID>
```
