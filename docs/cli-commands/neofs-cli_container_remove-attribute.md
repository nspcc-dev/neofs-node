## neofs-cli container remove-attribute

Remove container attribute

### Synopsis

Remove contaier attribute

```
neofs-cli container remove-attribute [flags]
```

### Options

```
      --address string        Address of wallet account
      --attribute string      attribute to be set
      --cid string            Container ID.
  -g, --generate-key          Generate new private key
  -h, --help                  help for remove-attribute
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --session string        Filepath to a JSON- or binary-encoded token of the container REMOVEATTRIBUTE session
  -t, --timeout duration      Timeout for the operation (default 15s)
      --ttl uint32            TTL value in request meta header (default 2)
      --valid-for duration    request validity duration (default 1m0s)
  -w, --wallet string         Path to the wallet
  -x, --xhdr strings          Request X-Headers in form of Key=Value
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli container](neofs-cli_container.md)	 - Operations with containers

