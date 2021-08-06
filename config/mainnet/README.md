# N3 Mainnet Storage node configuration

Here is the template for simple storage node configuration in N3 Mainnet.
Make sure to specify correct values instead of `<...>` placeholders. 
Do not change `contracts` section. Run latest neofs-node release with
fixed config `neofs-node -c config.yml`

## Tips

Use `grpcs://` scheme in announced address if you enable TLS in grpc server.
```yaml
node:
  addresses:
    - grpcs://neofs.my.org:8080

grpc:
  num: 1
  0:
    endpoint: neofs.my.org:8080
    tls:
      enabled: true
      certificate: /path/to/cert
      key: /path/to/key
```
