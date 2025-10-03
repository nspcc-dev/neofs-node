## neofs-cli container list

List all created containers

### Synopsis

List all created containers

```
neofs-cli container list [flags]
```

### Options

```
      --address string        Address of wallet account
  -g, --generate-key          Generate new private key
  -h, --help                  help for list
      --owner string          Owner of containers (omit to use owner from private key or if no key provided - list all containers)
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
  -t, --timeout duration      Timeout for the operation (default 15s)
      --ttl uint32            TTL value in request meta header (default 2)
  -w, --wallet string         Path to the wallet
      --with-attr             Request and print attributes of each container
  -x, --xhdr strings          Request X-Headers in form of Key=Value
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli container](neofs-cli_container.md)	 - Operations with containers

