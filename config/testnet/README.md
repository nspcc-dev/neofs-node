# NEO testnet-preview4 storage node configuration

There is a prepared configuration for NeoFS storage node deployment in
NEO testnet-preview4. The Easiest way to deploy storage node is to use prepared
docker image of storage node and run it with docker-compose.

## Build image

To build custom neofs-storage node image for NEO testnet-preview4:

```
neofs-node$ make image-storage-testnet
...
Successfully built 80ef4e3c488d
Successfully tagged nspccdev/neofs-storage-testnet:0.14.1-dirty
$
```

## Deploy node

To run storage node in NEO testnet-preview4 environment you should deposit
GAS assets, update docker-compose file and run it.

### Deposit

Storage node holder should deposit assets because it generates a bit of 
sidechain GAS for node's wallet. Sidechain GAS used to send bootstrap tx. 

To make a deposit invoke `deposit` method of NeoFS contract in testnet-preview4.
There are three arguments in this method:
- scripthash of the wallet
- amount of GAS (in decimal)
- scripthash of the storage node wallet.


NeoFS contract scripthash in NEO testnet-preview4 is `121da848e5239d24353c7b567a719d27e0fe7c06`

Last argument can be empty if you want to use
wallet key as storage node key. See a deposit example with `neo-go`:

```
neo-go contract invokefunction -r http://neo3-preview.go.nspcc.ru:20332 \
-w wallet.json -a NcrE6C1mvScQpAnFctK1Mw7P7i1buLCKav \
121da848e5239d24353c7b567a719d27e0fe7c06 \
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