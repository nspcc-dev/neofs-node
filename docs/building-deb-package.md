# Building Debian package on host

## Prerequisites

For now, we're assuming building for Debian 11 (stable) x86_64 on Debian 11 x86_64.

Go version 18.4 or later should be installed from official Go repos binaries should be buildable, i.e. this should run successfully:

* `make all`

## Installing packaging dependencies

```shell
$ sudo apt install debhelper-compat dh-sequence-bash-completion devscripts
```

Warining: number of package installed is pretty large considering dependecies.

## Package building

```shell
$ make debpackage
```

## Leftovers cleaning

```shell
$ make debclean
```
or
```shell
$ dh clean
```

# Building in container

Build dependencies will be added to Go container and package will be built.

```shell
$ sudo make docker/debpackage
```

# Package versioning

By default, package version is based on product version and may also contain 
git tags and hashes.
Package version could be overwritten by setting PACK_VERSION variable before
build, Debian package versioining rules should be respected.

```shell
$ PACK_VERSION=0.32 make debpackge
```
