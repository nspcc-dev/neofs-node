# basenet service

The `basenet` service defines the common network to use as "Internet" to let
services communicate to external world. It's a bridge network connected to the
host machine, so all programs running on host can connect to services exposed to
`basenet_internet` from devenv containers.

## .env settings

### LOCAL_DOMAIN=neofs.devenv

Domain to use for all containers exposed to `basenet_internet`.

### IPV4_PREFIX=192.168.130

IPv4 /24 subnet to use for all containers exposed to `basenet_internet`. Last
octet will be defined in `docker-compose.yml` file for each container inside
service. For simplicity, each service reserves ten host addresses.

### CA_CERTS_TRUSTED_STORE=/usr/local/share/ca-certificates
Trusted store location to add node self-signed tls certificates.

## bastion container

There is a `bastion` container with debian 10 userspace to simplify access to
devenv services.

Run shell in bastion:

```
neofs-dev-env$ docker exec -ti bastion /bin/bash
root@bastion:/# ip a sh
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN group default qlen 1000
    link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
    inet 127.0.0.1/8 scope host lo
       valid_lft forever preferred_lft forever
1569: eth0@if1570: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP group default
    link/ether 02:42:c0:a8:82:0a brd ff:ff:ff:ff:ff:ff link-netnsid 0
    inet 192.168.130.10/24 brd 192.168.130.255 scope global eth0
       valid_lft forever preferred_lft forever
```
