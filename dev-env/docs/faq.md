# F.A.Q, tips and tricks

### How to see what's inside running container if there's no shell?

You can run any program in container's namespace using `nsenter` utility.

```
$ docker inspect -f '{{.State.Pid}}' fs.neo.org
27242

$ sudo nsenter -t 27242 -n netstat -antp
Active Internet connections (servers and established)
Proto Recv-Q Send-Q Local Address           Foreign Address         State       PID/Program name
tcp        0      0 127.0.0.11:43783        0.0.0.0:*               LISTEN      1376/dockerd
tcp6       0      0 :::443                  :::*                    LISTEN      27242/nginx: master
tcp6       0      0 :::80                   :::*                    LISTEN      27242/nginx: master
```
