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
3. `ByteString` with Neo address of the storage node's public key

## Private subnet entrance

By default, storage nodes do not belong to private groups. Any node wishing to
enter the private subnet of storage nodes must first find out the corresponding
domain name. To request a binding to a given subnet, a node needs to set
related domain name in its information about when registering in the network
map. The domain is set via `VerifiedNodesDomain` attribute. To be admitted to
the network, a node must be present in the access list.
