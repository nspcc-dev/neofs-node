# N3 testnet RC1 storage node configuration

There is a prepared configuration for NeoFS storage node deployment in
N3 testnet RC1. The easiest way to deploy storage node is to use prepared
docker image of storage node and run it with docker-compose.

## Build image

Prepared neofs-storage-testnet image is available at docker hub. 
However, if you need to rebuild it for some reason, run 
`make image-storage-testnet` command:

```
$ make image-storage-testnet
...
Successfully built 80ef4e3c488d
Successfully tagged nspccdev/neofs-storage-testnet:0.18.0-dirty
```

## Deploy node

To run storage node in N3 testnet RC1 environment you should deposit GAS assets, 
update docker-compose file and start the node.

### Deposit

Storage node holder should deposit assets because it generates a bit of 
side chain GAS in node's wallet. Side chain GAS used to send bootstrap tx. 

First obtain GAS in N3 testnet RC1 chain. You can do that with
[faucet](https://neowish.ngd.network/neo3/) service.


Then make a deposit by invoking `deposit` method of NeoFS contract in N3 testnet
RC1. There are three arguments in this method:
- scripthash of the wallet
- amount of GAS (in decimal)
- scripthash of the storage node wallet.


NeoFS contract scripthash in NEO testnet RC1 is `37a32e1bf20ed5141bc5748892a82f14e75a8e22`

Last argument can be empty if you want to use
wallet key as storage node key. See a deposit example with `neo-go`:

```
neo-go contract invokefunction -r https://rpc1.n3.nspcc.ru:20331 \
-w wallet.json -a NcrE6C1mvScQpAnFctK1Mw7P7i1buLCKav \
37a32e1bf20ed5141bc5748892a82f14e75a8e22 \
deposit \
0cbd9d3c3e3a3d12ff5b8bd0d3a0548c6eeac4b9 \
int:1 \
bytes: \
-- 0cbd9d3c3e3a3d12ff5b8bd0d3a0548c6eeac4b9
```

### Configure

Then configure docker-compose.yml file. Change endpoints values. Both of them
should contain your public IP.

```yaml
     environment:
       - NEOFS_NODE_ADDRESS=192.168.140.1:36512
       - NEOFS_GRPC_ENDPOINT=192.168.140.1:36512
```

Also set up your [UN/LOCODE](https://unece.org/trade/cefact/unlocode-code-list-country-and-territory) 
attribute.

```yaml
     environment:
       - NEOFS_NODE_ADDRESS=192.168.140.1:36512
       - NEOFS_GRPC_ENDPOINT=192.168.140.1:36512
       - NEOFS_NODE_ATTRIBUTE_1=UN-LOCODE:RU LED
```

You can check validity of your UN/LOCODE attribute in 
[NeoFS LOCODE database](https://github.com/nspcc-dev/neofs-locode-db/releases/tag/v0.1.0)
with neofs-cli.

```
$ neofs-cli util locode info --db ./locode_db --locode 'RU LED'
Country: Russia
Location: Saint Petersburg (ex Leningrad)
Continent: Europe
Subdivision: [SPE] Sankt-Peterburg
Coordinates: 59.53, 30.15
```

It is recommended to pass node's key as a file. To do so convert your wallet 
WIF to 32-byte hex (via `neofs-cli` for example) and save it in file.

```
// Print WIF in a 32-byte hex format
$ neofs-cli util keyer Kwp4Q933QujZLUCcn39tzY94itNQJS4EjTp28oAMzuxMwabm3p1s
PrivateKey      11ab917cd99170cb8d0d48e78fca317564e6b3aaff7f7058952d6175cdca0f56
PublicKey       02be8b2e837cab232168f5c3303f1b985818b7583682fb49026b8d2f43df7c1059
WIF             Kwp4Q933QujZLUCcn39tzY94itNQJS4EjTp28oAMzuxMwabm3p1s
Wallet3.0       NVKYb9UHYv9mf6gY6CkBgR5ZYPhtx5d9sr
ScriptHash3.0   65d667398a35820f421630e49f73d7ea34952e67
ScriptHash3.0BE 672e9534ead7739fe43016420f82358a3967d665

// Save 32-byte hex into a file
$ echo '11ab917cd99170cb8d0d48e78fca317564e6b3aaff7f7058952d6175cdca0f56' | xxd -r -p > my_wallet.key
```

Then specify path to this file in docker-compose
```yaml
     volumes:
      - neofs_storage:/storage
      - ./my_wallet.key:/node.key
```

You also can provide WIF directly with 
`NEOFS_NODE_KEY=Kwp4Q933QujZLUCcn39tzY94itNQJS4EjTp28oAMzuxMwabm3p1s`
env in docker-compose.

### Start

Run node with `docker-compose up` command and stop it with `docker-compose down`.

### Debug

To print node logs use `docker logs neofs-testnet`. To print debug messages in 
log, setup log level to debug with this env:

```yaml
     environment:
       - NEOFS_LOGGER_LEVEL=debug
```