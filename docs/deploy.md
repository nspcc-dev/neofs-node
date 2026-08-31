# Step-by-step private NeoFS deployment

This guide describes the current bare-metal setup flow for a private NeoFS
network. The basic workflow is intentionally simple: generate a network config,
create the required wallets with `neofs-adm`, start the N3/Alphabet nodes, then
bring up a Storage node and trigger a new epoch.

This document focuses on the `neofs-adm` part of the deployment. It does not
cover every node startup detail, but it does cover the configuration and wallet
steps that are required for a private installation.

## Prerequisites

To follow this guide you need:

- a recent release of [neo-go](https://github.com/nspcc-dev/neo-go/releases)
- a recent release of [neofs-adm](https://github.com/nspcc-dev/neofs-node/releases)
- the compiled NeoFS contracts from the latest [neofs-contract](https://github.com/nspcc-dev/neofs-contract/releases) release

## Step 1: Prepare the network configuration

A private network usually has a set of N3 consensus nodes, the same number of
Alphabet nodes, and any number of Storage nodes. The number of Storage nodes can
be scaled as needed, but the number of consensus and Alphabet nodes is chosen at
network setup time and should be planned ahead.

Create a config file with `neofs-adm` first:

```bash
$ neofs-adm config init --path foo.network.yml
Initial config file saved to foo.network.yml

$ cat foo.network.yml
rpc-endpoint: https://neo.rpc.node:30333
alphabet-wallets: /home/user/deploy/alphabet-wallets
network:
  max_object_size: 67108864
  epoch_duration: 240
  basic_income_rate: 0
  fee:
    candidate: 0
    container: 0
    withdraw: 0
credentials:
  az: hunter2
```

For a private installation, all network fees and the basic income rate are
usually set to `0`.

The epoch duration should match the expected block generation rate in the N3
sidechain. With the default 15 seconds per block, 240 blocks is roughly one
hour.

For the maximum object size, 64 MiB or 128 MiB is a reasonable value for a
private test or development network.

## Step 2: Generate the Alphabet wallet set

The same wallets are used for the consensus and Alphabet roles. `neofs-adm`
creates the needed accounts and multisig entries automatically:

```bash
$ neofs-adm -c foo.network.yml fschain generate-alphabet --size 1
size: 1
alphabet-wallets: /home/user/deploy/alphabet-wallets
wallet[0]: hunter2
```

Keep the wallet files and the network config in a safe place. Losing them means
losing access to the private network state.

## Step 3: Launch the consensus and Alphabet nodes

The generated wallets are then used to configure the N3 consensus nodes and the
Alphabet nodes.

The command output from `generate-alphabet` includes the wallet password. Use the
wallets from `alphabet-wallets` to populate the consensus configuration, and use
those same public keys for the `ProtocolConfiguration.StandbyCommittee` and
`fschain.validators` settings. Configuration examples for node startup are kept in
the [neo-go repository](https://github.com/nspcc-dev/neo-go/tree/master/config).

At this point, the consensus nodes should connect to each other and start
producing blocks. Additional dedicated RPC endpoints are recommended for storage
nodes, because using a consensus node for RPC traffic is not good practice for a
production-like setup.

## Step 4: Generate a storage wallet

Create a wallet for the Storage node and fund it for bootstrap:

```bash
$ neofs-adm -c foo.network.yml fschain generate-storage-wallet --storage-wallet ./sn01.json --initial-gas 10.0
New password >
Waiting for transactions to persist...
```

The generated wallet file can then be used by the Storage node configuration.

```yaml
node:
  wallet:
    path: "/home/user/deploy/sn01.json"
    address: "Ngr7p8Z9S22XDH6VkUG9oXobv8zZRAWwwv"
    password: "foobar"
```

## Step 5: Start the Storage node and activate it

Once the Storage node is configured, start it and wait for it to register in the
network map. If you want it to appear in the next epoch immediately, trigger a
new epoch:

```bash
$ neofs-adm -c foo.network.yml fschain force-new-epoch
Current epoch: 8, increase to 9.
Waiting for transactions to persist...
```

After that, the private NeoFS Storage deployment is ready to serve requests.
