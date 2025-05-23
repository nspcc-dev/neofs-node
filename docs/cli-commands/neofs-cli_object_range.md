## neofs-cli object range

Get payload range data of an object

### Synopsis

Get payload range data of an object

```
neofs-cli object range [flags]
```

### Options

```
      --address string        Address of wallet account
      --bearer string         File with signed JSON or binary encoded bearer token
      --cid string            Container ID.
      --file string           File to write object payload to. Default: stdout.
  -g, --generate-key          Generate new private key
  -h, --help                  help for range
      --oid string            Object ID.
      --range string          Range to take data from in the form offset:length
      --raw                   Set raw request option
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string        Filepath to a JSON- or binary-encoded token of the object RANGE session
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

