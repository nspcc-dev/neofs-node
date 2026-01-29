## neofs-cli container test-policy

Test placement policy

### Synopsis

Test placement policy parsing and validation.
Policy can be provided as QL-encoded string, JSON-encoded string or path to file with it.
Shows nodes that will be used for container placement based on current network map snapshot.

```
neofs-cli container test-policy [flags]
```

### Options

```
  -h, --help                  help for test-policy
  -p, --policy string         QL-encoded or JSON-encoded placement policy or path to file with it
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --short                 Shortens output of node info
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli container](neofs-cli_container.md)	 - Operations with containers

