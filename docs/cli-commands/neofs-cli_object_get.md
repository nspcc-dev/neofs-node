## neofs-cli object get

Get object from NeoFS

### Synopsis

Get object from NeoFS

```
neofs-cli object get [flags]
```

### Options

```
      --address string        Address of wallet account
      --bearer string         File with signed JSON or binary encoded bearer token
      --binary                Serialize whole object structure into given file(id + signature + header + payload).
      --cid string            Container ID.
      --file string           File to write object payload to(with -b together with signature and header). Default: stdout.
  -g, --generate-key          Generate new private key
  -h, --help                  help for get
      --no-progress           Do not show progress bar
      --oid string            Object ID.
      --raw                   Set raw request option
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string        Filepath to a JSON- or binary-encoded token of the object GET session
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

