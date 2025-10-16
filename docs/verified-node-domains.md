# Verified domains of the NeoFS storage nodes

Storage nodes declare information flexibly via key-value string attributes when
applying to enter the NeoFS network map. In general, any attributes can be
declared, however, some of them may be subject to restrictions. In particular,
some parties may need to limit the relationship to them of any nodes of their
public network. For example, an organization may need to deploy its storage
nodes as a subnet of a public network to implement specific data storage
strategies. In this example, the organization’s nodes will be “normal” for 3rd
parties, while other nodes will not be able to enter the subnet without special
permission at the system level.

NeoFS implements solution of the described task through access lists managed
within NeoFS NNS.

## Access lists

These lists are stored in the NeoFS NNS. Each party may register any available
NNS domain and set records of `TXT` type with Neo addresses of the storage
nodes. After the domain is registered, it becomes an alias to the subnet composed
only from specified storage nodes. Any storage node trying to associate itself
with this subnet while trying to enter the network must have public key
presented in the access list. The Inner Ring will deny everyone else access to
the network map.

### Domain record format

For each public key, a record is created - a structure with at least 3 fields:
1. `ByteString` with name of the corresponding domain
2. `Integer` that is `16` for TXT records (other record types are allowed but left unprocessed)
3. `ByteString` with `address=<Neo address>` value described in [NEP-18 Specification](https://github.com/neo-project/proposals/pull/133)

### Management

NeoFS ADM tool may be used to work with verified nodes' domains from command line.
```
$ neofs-adm fschain verified-nodes-domain
```

#### Get access list

List allowed storage nodes:
```
$ neofs-adm fschain verified-nodes-domain access-list -r https://rpc1.morph.t5.fs.neo.org:51331 \
-d nodes.some-org.neofs
NZ1czz5gkEDamTg6Tiw6cxqp9Me1KLs8ae
NfMvD6WmBiCr4erfEnFFLs7jdj4Y5CM7nN
```
where `-r` is the NeoFS Sidechain network endpoint.

See command help for details
```
$ neofs-adm fschain verified-nodes-domain access-list -h
```

#### Set access list

Set list of Neo addresses of the allowed storage nodes:
```
$ neofs-adm fschain verified-nodes-domain set-access-list -r https://rpc1.morph.t5.fs.neo.org:51331 \
-d nodes.some-org.neofs -w path-to-org-wallet.json \
--neo-addresses NZ1czz5gkEDamTg6Tiw6cxqp9Me1KLs8ae \
--neo-addresses NfMvD6WmBiCr4erfEnFFLs7jdj4Y5CM7nN
Enter password for NTFWBaa1NxhJR1VjAqvLpkZidXgc3oefmj >
Waiting for transactions to persist...
Access list has been successfully updated.
```
where `path-to-org-wallet.json` is a `some-org.neofs` domain admin wallet.

Auxiliary flag `--public-keys` allows you to specify public keys instead of addresses:
```
$ neofs-adm fschain verified-nodes-domain set-access-list -r https://rpc1.morph.t5.fs.neo.org:51331 \
-d nodes.some-org.neofs -w path-to-org-wallet.json \
--public-keys 02b3622bf4017bdfe317c58aed5f4c753f206b7db896046fa7d774bbc4bf7f8dc2 \
--public-keys 02103a7f7dd016558597f7960d27c516a4394fd968b9e65155eb4b013e4040406e
Enter password for NTFWBaa1NxhJR1VjAqvLpkZidXgc3oefmj >
Waiting for transactions to persist...
Access list has been successfully updated.
```

See command help for details:
```
$ neofs-adm fschain verified-nodes-domain set-access-list -h
```

## Private subnet entrance

By default, storage nodes do not belong to private groups. Any node wishing to
enter the private subnet of storage nodes must first find out the corresponding
domain name. To request a binding to a given subnet, a node needs to set
related domain name in its information about when registering in the network
map. The domain is set via `VerifiedNodesDomain` attribute. To be admitted to
the network, a node must be present in the access list.
