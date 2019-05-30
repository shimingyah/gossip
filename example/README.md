# Example

A basic key/val memory store of how to use gossip.

## Install

```
go get github.com/shimingyah/gossip
go build gossip.go delegate.go
```

## Cluster

start first node

```
./gossip -gossipPort=60001 -httpPort=8081
```

start second node join the first node

```
./gossip -gossipPort=60002 -httpPort=8082 -join=127.0.0.1:60001
```

start third node

```
./gossip -gossipPort=60003 -httpPort=8083 -join=127.0.0.1:60001,127.0.0.1:60002
```
the three nodes will communicate with each other.

## Http API
* /has - check key if is exist
* /get - get value
* /set - set key value
* /del - del key

use example:

```
# set
curl "http://127.0.0.1:8081/set?key=foo&val=bar"
# get
curl "http://127.0.0.1:8081/get?key=foo"
# has
curl "http://127.0.0.1:8081/has?key=foo"
# del
curl "http://127.0.0.1:8081/del?key=foo"
```

## Consistency check
fisrt set key on http server 8081, set k-v successful.

```
curl "http://127.0.0.1:8081/set?key=foo&val=bar"
```
second get key on http server 8082, you can get it.

```
curl "http://127.0.0.1:8082/get?key=foo"
```
third del key on http server 8083, del k-v successful.

```
curl "http://127.0.0.1:8082/del?key=foo"
```
fourthly has key on http server 8082, it will return not found.

```
curl "http://127.0.0.1:8082/has?key=foo"
```
the end get key on http server 8081, it will return not found.

```
curl "http://127.0.0.1:8081/get?key=foo"
```