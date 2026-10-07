# Notary service in chains

[Notary service](https://github.com/neo-project/neo/issues/1573#issuecomment-704874472)
is a service that provides on-chain network assistance to form multisignature 
transactions. Inner Ring (Alphabet) nodes use multisignature transactions to
create containers, approve balance changes, update network map, tick epochs, 
etc. With notary service, it takes up to seven times fewer transactions 
to do these operations. Notary service calculates the exact amount of GAS 
to execute transaction, therefore operations are cheaper (withdraw fee **with**
notary is less than 0.5 GAS; withdraw fee **without** notary is up to 7.0 GAS). 

By default, main chain service is running without notary service. However, you
can change that in configuration.

# Enable notary service in main chain

To enable notary service in main chain do these steps.

1. Deploy processing contract to the chain.

See up/bootstrap target in the Makefile and set its hash as a parameter during
neofs contract deployment.

Set notary_disabled to false for neofs deployments at the same time.

2. Update `service/chain/protocol.privnet.yml` and enable notary settings.

```yaml
ProtocolConfiguration:
  P2PSigExtensions: true
ApplicationConfiguration:
  P2PNotary:
    Enabled: true
```

Main chain generates a block once per 15 seconds, so Inner Ring takes about 
15-30 seconds to make a notary deposit in main chain after startup. Then 
neofs-dev-env is ready to work.
