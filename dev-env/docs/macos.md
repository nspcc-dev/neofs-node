# Setting up DevEnv on macOS

## The Problem

Currently Docker for macOS has no support for network routing into the Host
Virtual Machine that is created using hyperkit. The reason for this is due to
the fact that the network interface options used to create the instance does not
create a bridge interface between the Physical Machine and the Host Virtual
Machine. To make matters worse, the arguments used to create the Host Virtual
Machine is hardcoded into the Docker for macOS binary with no means to configure
it.

## How to setup DevEnv on macOS

- Clone https://github.com/AlmirKadric-Published/docker-tuntap-osx
```sh
$ git clone git@github.com:AlmirKadric-Published/docker-tuntap-osx.git
```

- Install tuntap for macOS
```
$ brew tap caskroom/cask
$ brew cask install tuntap
```

- Restart macOS and allow tuntap kext in settings before

- Docker for macOS should be run before

- Install docker-tuntap. This will automatically check if the currently
  installed shim is the correct version and make a backup if necessary

```
$ ./sbin/docker_tap_install.sh
```

- After this you will need to bring up the network interfaces every time the
  docker Host Virtual Machine is restarted

```
$ ./sbin/docker_tap_up.sh
```

- Bootup devenv

- See IPV4_PREFIX, for example, now it's
```
IPV4_PREFIX=192.168.130
```

- Add route to devenv (<IPV4_PREFIX>.0, for example IPV4_PREFIX=192.168.130)
```
$ sudo route add -net 192.168.130.0 -netmask 255.255.255.0 10.0.75.2
```

## How to uninstall

The uninstall script will simply revert the installer. Restoring the original
and removing the shim:

```
$ ./sbin/docker_tap_uninstall.sh
```

Remove route to devenv (<IPV4_PREFIX>.0, for example IPV4_PREFIX=192.168.130)
```
$ sudo route delete -net 192.168.130.0 -netmask 255.255.255.0 10.0.75.2
```

## Restart macOS or upgrade Docker for macoS

When you restart macOS or install new version of Docker, you should do next
steps:
- reinstall docker-tuntap forced
```
$ ./sbin/docker_tap_install.sh -f
```

- wait until docker will be restarted

- up tuntap interface
```
$ ./sbin/docker_tap_up.sh
```

- bootup devenv

- Add route to devenv (<IPV4_PREFIX>.0, for example IPV4_PREFIX=192.168.130)
```
$ sudo route add -net 192.168.130.0 -netmask 255.255.255.0 10.0.75.2
```

## How it works

This installer (`docker_tap_install.sh`) will move the original hyperkit binary
(`hyperkit.original`) inside the Docker for macOS application and places our shim
(`./sbin/docker.hyperkit.tuntap.sh`) in it's stead. This shim will then inject
the additional arguments required to attach a
[TunTap](http://tuntaposx.sourceforge.net/) interface into the Host Virtual
Machine, creating a bridge interface between the guest and the host (this is
essentially what hvint0 is on Docker for Windows).

From there the `up` script (`docker_tap_up.sh`) is used to bring the network
interface up on both the Physical Machine and the Host Virtual Machine. Unlike
the install script, which only needs to be run once, this `up` script must be
run for every restart of the Host Virtual Machine.

Once done, the IP address `10.0.75.2` can be used as a network routing gateway
to reach any containers within the Host Virtual Machine:

```
$ route add -net <IP RANGE> -netmask <IP MASK> 10.0.75.2
```

**Note:** Although as of docker-for-mac version `17.12.0` you do not need the
following, for prior versions you will need to setup IP Forwarding in the
iptables defintion on the Host Virtual Machine:

(This is not done by the helpers as this is not a OSX or tuntap specific issue.
You would need to do the same for Docker for Windows, as such it should be
handled outside the scope of this project.)

```
$ docker run --rm --privileged --pid=host debian nsenter -t 1 -m -u -n -i iptables -A FORWARD -i eth1 -j ACCEPT
```

**Note:** Although not required for docker-for-mac versions greater than
`17.12.0`, the above command can be replaced with the following if ever needed
and is tested to be working on Docker for Windwos as an alternative. This is in
case Docker for macOS changes something in future and this command ends up being a
necessity once again.

```
$ docker run --rm --privileged --pid=host docker4w/nsenter-dockerd /bin/sh -c 'iptables -A FORWARD -i eth1 -j ACCEPT'
```

## Dependencies
- [Docker for Mac](https://www.docker.com/docker-mac)
- [TunTap](http://tuntaposx.sourceforge.net/)
- [Docker TunTap](https://github.com/AlmirKadric-Published/docker-tuntap-osx)
