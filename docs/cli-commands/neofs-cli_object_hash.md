## neofs-cli object hash

Get object hash

### Synopsis

Get object hash

```
neofs-cli object hash [flags]
```

### Options

```
      --address string        Address of wallet account
      --bearer string         File with signed JSON or binary encoded bearer token
      --cid string            Container ID.
  -g, --generate-key          Generate new private key
  -h, --help                  help for hash
      --oid string            Object ID.
      --range string          Range to take hash from in the form offset1:length1,... Full object payload length if not specified
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --salt string           Salt in hex format
      --session string        Filepath to a JSON- or binary-encoded token of the object RANGEHASH session
  -t, --timeout duration      Timeout for the operation (default 15s)
      --ttl uint32            TTL value in request meta header (default 2)
      --type string           Hash type. Either 'sha256' or 'tz' (default "sha256")
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

