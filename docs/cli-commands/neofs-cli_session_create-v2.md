## neofs-cli session create-v2

Create V2 session token

### Synopsis

Create V2 session token with subjects, contexts, and verbs.

V2 tokens support:
- Multiple subjects (accounts authorized to use the token)
- Multiple contexts (container + object operations)
- No server-side session key storage (no SessionCreate RPC needed)
- Delegation chains (future feature)

Example usage:
  neofs-cli session create-v2 \
    --wallet wallet.json \
    --r node.neofs.network:8080 \
    --lifetime 100 \
    --out token.json \
    --json \
    --subject NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM \
    --container 5HqniP5vq5xXr3FdijTSekrQJHu1WnADt2uLg7KSViZM \
    --verbs GET,HEAD,SEARCH

Default lifetime of session token is 10 epochs
if none of --expire-at or --lifetime flags is specified.


```
neofs-cli session create-v2 [flags]
```

### Options

```
      --address string        Address of wallet account
      --container string      Container ID for the context
  -e, --expire-at uint        The last active epoch for token to stay valid
  -h, --help                  help for create-v2
      --json                  Output token in JSON
  -l, --lifetime uint         Number of epochs for token to stay valid (default 10)
      --objects strings       Object IDs for the context (empty = all objects in container)
      --out string            File to write session token to
  -r, --rpc-endpoint string   Remote node address (as 'multiaddr' or '<host>:<port>')
      --subject strings       Subject user IDs (can be specified multiple times)
      --subject-nns strings   Subject NNS names (can be specified multiple times)
      --verbs string          Comma-separated list of verbs (GET,PUT,HEAD,SEARCH,DELETE,RANGE,RANGEHASH,CONTAINERSET,CONTAINERPUT,CONTAINERDELETE)
  -w, --wallet string         Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli session](neofs-cli_session.md)	 - Operations with session token

