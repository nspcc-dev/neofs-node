# N3 Testnet Storage node configuration

There is a prepared configuration for NeoFS Storage Node deployment in
N3 Testnet. The easiest way to deploy Storage Node is to use prepared
docker image and run it with docker-compose.

## Build image

Prepared **neofs-storage-testnet** image is available at Docker Hub. 
However, if you need to rebuild it for some reason, run 
`make image-storage-testnet` command.

```
$ make image-storage-testnet
...
Successfully built ab0557117b02
Successfully tagged nspccdev/neofs-storage-testnet:0.25.1
```

## Deploy node

To run storage node in N3 Testnet environment you should deposit GAS assets, 
update docker-compose file and start the node.

### Deposit

Storage Node owner should deposit GAS to NeoFS smart contract. It generates a 
bit of side chain GAS in node's wallet. Side chain GAS used to send bootstrap tx. 

First obtain GAS in N3 Testnet chain. You can do that with
[faucet](https://neowish.ngd.network) service.

Then make a deposit by transferring GAS to NeoFS contract in N3 Testnet.
You can provide scripthash in the `data` argument of transfer tx to make a
deposit to specified account. Otherwise, deposit is made to tx sender.

NeoFS contract scripthash in N3 Testnet is `b65d8243ac63983206d17e5221af0653a7266fa1`, 
so the address is `NadZ8YfvkddivcFFkztZgfwxZyKf1acpRF`.

See a deposit example with `neo-go`.

```
neo-go wallet nep17 transfer -w wallet.json -r https://rpc01.testnet.n3.nspcc.ru:21331 \
--from NXxRAFPqPstaPByndKMHuC8iGcaHgtRY3m \
--to NadZ8YfvkddivcFFkztZgfwxZyKf1acpRF \
--token GAS \
--amount 1
```

### Configure

Then configure `node_config.env` file. Change endpoints values. Both
should contain your **public** IP.

```
NEOFS_GRPC_0_ENDPOINT=65.52.183.157:36512
NEOFS_NODE_ADDRESSES=65.52.183.157:36512
```

Set up your [UN/LOCODE](https://unece.org/trade/cefact/unlocode-code-list-country-and-territory) 
attribute.

```
NEOFS_GRPC_0_ENDPOINT=65.52.183.157:36512
NEOFS_NODE_ADDRESSES=65.52.183.157:36512
NEOFS_NODE_ATTRIBUTE_2=UN-LOCODE:RU LED
```

You can validate UN/LOCODE attribute in 
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
WIF to 32-byte hex (via `neofs-cli` for example) and save it to file.

```
// Print WIF in a 32-byte hex format
$ neofs-cli util keyer Kwp4Q933QujZLUCcn39tzY94itNQJS4EjTp28oAMzuxMwabm3p1s
PrivateKey      11ab917cd99170cb8d0d48e78fca317564e6b3aaff7f7058952d6175cdca0f56
PublicKey       02be8b2e837cab232168f5c3303f1b985818b7583682fb49026b8d2f43df7c1059
WIF             Kwp4Q933QujZLUCcn39tzY94itNQJS4EjTp28oAMzuxMwabm3p1s
Wallet3.0       Nfzmk7FAZmEHDhLePdgysQL2FgkJbaEMpQ
ScriptHash3.0   dffe39998f50d42f2e06807866161cd0440b4bdc
ScriptHash3.0BE dc4b0b44d01c16667880062e2fd4508f9939fedf

// Save 32-byte hex into a file
$ echo '11ab917cd99170cb8d0d48e78fca317564e6b3aaff7f7058952d6175cdca0f56' | xxd -r -p > my_wallet.key
```

Then specify path to this file in `docker-compose.yml`
```yaml
     volumes:
      - neofs_storage:/storage
      - ./my_wallet.key:/node.key
```


NeoFS objects will be stored on your machine. By default, docker-compose 
configured to store objects in named docker volume `neofs_storage`. You can 
specify directory on the filesystem to store objects there.

```yaml
     volumes:
       - /home/username/neofs/rc3/storage:/storage
       - ./my_wallet.key:/node.key
```

### Start

Run node with `docker-compose up` command and stop it with `docker-compose down`.

### Debug

To print node logs use `docker logs neofs-testnet`. To print debug messages in 
log, setup log level to debug with this env:

```yaml
     environment:
       - NEOFS_LOGGER_LEVEL=debug
```
