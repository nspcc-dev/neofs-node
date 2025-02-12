## neofs-cli object searchv2

Search object (new)

### Synopsis

Search object (new)

```
neofs-cli object searchv2 [flags]
```

### Options

```
      --address string        Address of wallet account
      --attributes strings    Additional attributes to display for suitable objects
      --bearer string         File with signed JSON or binary encoded bearer token
      --cid string            Container ID.
      --count uint16          Max number of resulting items. Must not exceed 1000
      --cursor string         Cursor to continue previous search
  -f, --filters strings       Repeated filter expressions or files with protobuf JSON
  -g, --generate-key          Generate new private key
  -h, --help                  help for searchv2
      --oid string            Search object by identifier
      --phy                   Search physically stored objects
      --root                  Search for user objects
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string        Filepath to a JSON- or binary-encoded token of the object SEARCH session
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

* [neofs-cli object](neofs-cli_object.md)	 - Operations with Objects

