## neofs-cli session create-v2

Create V2 session token

### Synopsis

Create V2 session token with subjects and multiple contexts.

V2 tokens always create a server-side key via SessionCreate RPC
and include it as the last subject in the token.

V2 tokens support:
- Multiple subjects (accounts authorized to use the token)
- Multiple contexts (container + object operations)
- Token delegation chains via --origin flag

Context format: containerID:verbs
- containerID: Container ID or "0" for wildcard (any container)
- verbs: Comma-separated list of operations (e.g., DELETE,GET,HEAD,PUT,SEARCH)

Example usage:
  neofs-cli session create-v2 \
    --wallet wallet.json \
    --rpc node.neofs.devenv:8080 \
    --lifetime 10000 \
    --out token.json \
    --json \
    --subject NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM \
	--context 0:CONTAINERPUT \
    --context 5HqniP5vq5xXr3FdijTSekrQJHu1WnADt2uLg7KSViZM:SEARCH
	--origin original-token.json

Default lifetime of session token is 36000 seconds
if none of --expire-at or --lifetime flags is specified.


```
neofs-cli session create-v2 [flags]
```

### Options

```
      --address string            Address of wallet account
      --context stringArray       Context spec (repeatable): containerID:verbs. Use '0' for wildcard container.
  -e, --expire-at uint            Expiration time in seconds for token to stay valid
      --final                     Set the final flag in the token, disallowing further delegation
  -f, --force                     Skip token validation (use with caution)
  -h, --help                      help for create-v2
      --json                      Output token in JSON
  -l, --lifetime uint             Duration in seconds for token to stay valid (default 36000)
      --origin string             Path to origin token file for token delegation chain
      --out string                File to write session token to
  -r, --rpc-endpoint string       Remote node address (as 'multiaddr' or '<host>:<port>')
      --subject stringArray       Subject user IDs (can be specified multiple times)
      --subject-nns stringArray   Subject NNS names (can be specified multiple times)
  -w, --wallet string             Path to the wallet
```

### Options inherited from parent commands

```
  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
  -v, --verbose         Verbose output
```

### SEE ALSO

* [neofs-cli session](neofs-cli_session.md)	 - Operations with session token

