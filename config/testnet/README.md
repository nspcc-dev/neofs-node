# N3 testnet RC2 storage node configuration

There is a prepared configuration for NeoFS storage node deployment in
N3 testnet RC2. The easiest way to deploy storage node is to use prepared
docker image of storage node and run it with docker-compose.

## Build image

Prepared neofs-storage-testnet image is available at docker hub. 
However, if you need to rebuild it for some reason, run 
`make image-storage-testnet` command:

```
$ make image-storage-testnet
...
Successfully built 80ef4e3c488d
Successfully tagged nspccdev/neofs-storage-testnet:0.20.0-dirty
```

## Deploy node

To run storage node in N3 testnet RC2 environment you should deposit GAS assets, 
update docker-compose file and start the node.

### Deposit

Storage node holder should deposit assets because it generates a bit of 
side chain GAS in node's wallet. Side chain GAS used to send bootstrap tx. 

First obtain GAS in N3 testnet RC2 chain. You can do that with
[faucet](https://neowish.ngd.network) service.

Then make a deposit by transferring GAS to NeoFS contract in N3 testnet RC2.
You can provide scripthash in the `data` argument of transfer tx to make a
deposit to specified account. Otherwise deposit is made to tx sender.

NeoFS contract scripthash in NEO testnet RC2 is `088c76a08c7b4546582fe95df1ba58f61f165645`, 
so the address is `NSEawP75SPnnH9sRtk18xJbjYGHu2q5m1W`

See a deposit example with `neo-go`:

```
neo-go wallet nep17 transfer -w wallet.json -r https://rpc1.n3.nspcc.ru:20331 \
--from NXxRAFPqPstaPByndKMHuC8iGcaHgtRY3m \
--to NSEawP75SPnnH9sRtk18xJbjYGHu2q5m1W \
--token GAS \
--amount 1
```

### Configure

Then configure docker-compose.yml file. Change endpoints values. Both of them
should contain your **public** IP.

```yaml
     environment:
       - NEOFS_NODE_ADDRESS=65.52.183.157:36512
       - NEOFS_GRPC_ENDPOINT=65.52.183.157:36512
```

Set up your [UN/LOCODE](https://unece.org/trade/cefact/unlocode-code-list-country-and-territory) 
attribute.

```yaml
     environment:
       - NEOFS_NODE_ADDRESS=65.52.183.157:36512
       - NEOFS_GRPC_ENDPOINT=65.52.183.157:36512
       - NEOFS_NODE_ATTRIBUTE_1=UN-LOCODE:RU LED
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

Then specify path to this file in docker-compose
```yaml
     volumes:
      - neofs_storage:/storage
      - ./my_wallet.key:/node.key
```

Another way is to provide WIF directly with env in docker-compose.
```
NEOFS_NODE_KEY=Kwp4Q933QujZLUCcn39tzY94itNQJS4EjTp28oAMzuxMwabm3p1s
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