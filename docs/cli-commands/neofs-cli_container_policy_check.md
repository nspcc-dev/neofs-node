## neofs-cli container policy check

Check placement policy

### Synopsis

Check placement policy parsing and validation.
Policy can be provided as QL-encoded string, JSON-encoded string or path to file with it.
Shows nodes that will be used for container placement based on current network map snapshot.
Note: this command uses a zero container ID for placement; when there are many nodes and
only a subset must be chosen, results may differ from a real container.

```
neofs-cli container policy check [flags]
```

### Options

```
  -h, --help                  help for check
  -p, --policy string         QL-encoded or JSON-encoded placement policy or path to file with it
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --short                 Shortens output of node info
  -t, --timeout duration      Timeout for the operation (default 15s)
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli container policy](neofs-cli_container_policy.md)	 - Operations with container placement policy

