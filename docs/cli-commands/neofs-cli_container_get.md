## neofs-cli container get

Get container field info

### Synopsis

Get container field info

```
neofs-cli container get [flags]
```

### Options

```
      --address string        Address of wallet account
      --cid string            Container ID.
      --from string           Path to file with encoded container
  -g, --generate-key          Generate new private key
  -h, --help                  help for get
      --json                  Print or dump container in JSON format
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
  -t, --timeout duration      Timeout for the operation (default 15s)
      --to string             Path to dump encoded container
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

* [neofs-cli container](neofs-cli_container.md)	 - Operations with containers

