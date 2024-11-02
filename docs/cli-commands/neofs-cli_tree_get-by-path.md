## neofs-cli tree get-by-path

Get a node by its path

```
neofs-cli tree get-by-path [flags]
```

### Options

```
      --address string        Address of wallet account
      --cid string            Container ID.
  -g, --generate-key          Generate new private key
  -h, --help                  help for get-by-path
      --latest                Look only for the latest version of a node
      --path string           Path to a node
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --tid string            Tree ID
  -t, --timeout duration      Timeout for the operation (default 15s)
  -w, --wallet string         Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli tree](neofs-cli_tree.md)	 - Operations with the Tree service

