# NeoFS Inner Ring

NeoFS Inner Ring (Alphabet) node. According to governance scheme, Inner Ring
should contain Alphabet nodes that share key with one of side chain consensus
nodes. In basic setup there is a single Inner Ring (Alphabet) node running
consensus node internally.

Deployed contracts:
- [executables](https://github.com/nspcc-dev/neofs-node/tree/v0.39.1/contracts).
- [source code](https://github.com/nspcc-dev/neofs-contract/tree/v0.19.1)

N3 RPC service is served on `http://ir01.neofs.devenv:30333`.

## .env settings

### IR_VERSION

Image version label to use for Inner Ring docker containers.

If you want to use locally built image, just set it's label here. Instead of
pulling from DockerHub, the local image will be used.

### IR_IMAGE=nspccdev/neofs-ir

Image label prefix to use for Inner Ring docker containers.

### IR_NUMBER_OF_NODES

The number of IR nodes that will work. The value must be either 1, 4, or 7.

## NeoFS global config

NeoFS uses global configuration to store epoch duration, maximum object size,
container fee and other network parameters. Global configuration is recorded in
NeoFS chain and managed by Inner Ring (Alphabet) nodes.

To change these parameters use `make update.*` commands. Command down below
changes epoch duration from 300 blocks (about 300 seconds with 1bps) to 30.
Script enters passwords automatically with `expect` utility.

```
$ make update.epoch_duration val=30
Changing EpochDuration configuration value to 30
Enter account NfgHwwTi3wHAS8aFAN243C5vGbkYDpqLHP password >
Sent invocation transaction bdc0fa88cd6719ef6df2b9c82de423ddec6141ca24255c2d0072688083b1de9d
Updating NeoFS epoch to 20
Enter account NfgHwwTi3wHAS8aFAN243C5vGbkYDpqLHP password >
Sent invocation transaction 12296e1ce24dd6c04edb9c56d0a1d0e26d3226adefb0333c74a28788f44a8d0f
```

Read more about available configuration in Makefile help.

```
$ make help
...
Targets:
...
update.basic_income_rate        Update basic income rate in fixed 12 (make update.basic_income_rate val=1000)
update.container_fee            Update container fee per alphabet node in fixed 12 (make update.container_fee val=500)
update.eigen_trust_iterations   Update amount of EigenTrust iterations (make update.eigen_trust_iterations val=2)
update.epoch_duration           Update epoch duration in side chain blocks (make update.epoch_duration val=30)
update.max_object_size          Update max object size in bytes (make update.max_object_size val=1000)
```
