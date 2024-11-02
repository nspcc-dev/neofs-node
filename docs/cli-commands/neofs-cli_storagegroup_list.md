## neofs-cli storagegroup list

List storage groups in NeoFS container

### Synopsis

List storage groups in NeoFS container

```
neofs-cli storagegroup list [flags]
```

### Options

```
      --address string        Address of wallet account
      --bearer string         File with signed JSON or binary encoded bearer token
      --cid string            Container ID.
  -g, --generate-key          Generate new private key
  -h, --help                  help for list
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
  -t, --timeout duration      Timeout for the operation (default 15s)
      --ttl uint32            TTL value in request meta header (default 2)
  -w, --wallet string         Path to the wallet
  -x, --xhdr strings          Request X-Headers in form of Key=Value
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli storagegroup](neofs-cli_storagegroup.md)	 - Operations with Storage Groups

