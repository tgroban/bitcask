# bitcask

[![Build Status](https://ci.mills.io/api/badges/prologic/bitcask/status.svg)](https://ci.mills.io/prologic/bitcask)
[![Go Report Card](https://goreportcard.com/badge/go.mills.io/bitcask)](https://goreportcard.com/report/go.mills.io/bitcask)
[![Go Reference](https://pkg.go.dev/badge/go.mills.io/bitcask.svg)](https://pkg.go.dev/go.mills.io/bitcask)

A high performance Key/Value store written in [Go](https://golang.org) with a predictable read/write performance and high throughput. Uses a [Bitcask](https://en.wikipedia.org/wiki/Bitcask) on-disk layout (LSM+WAL) similar to [Riak](https://riak.com/)

For a more feature-complete Redis-compatible server, distributed key/value store have a look at [Bitraft](https://git.mills.io/prologic/bitraft) which uses this library as its backend. Use [Bitcask](https://go.mills.io/bitcask) as a starting point or if you want to embed in your application, use [Bitraft](https://git.mills.io/prologic/bitraft) if you need a complete server/client solution with high availability with a Redis-compatible API.

## Features

* Embedded (`import "go.mills.io/bitcask"`)
* Builtin CLI (`bitcask`)
* Builtin Redis-compatible server (`bitcaskd`)
* Predictable read/write performance
* Low latency
* High throughput (See: [Performance](README.md#Performance) )

## Is Bitcask right for my project?

__NOTE__: Please read this carefully to identify whether using Bitcask is
          suitable for your needs.

`bitcask` is a **great fit** for:

- Storing hundreds of thousands to millions of key/value pairs based on
  default configuration. With the default configuration (_configurable_)
  of 64 bytes per key and 64kB values, 1M keys would consume roughly ~600-700MB
  of memory ~65-70GB of disk storage. These are all configurable when you
  create a new database with `bitcask.Open(...)` with functional-style options
  you can pass with `WithXXX()`.

- As the backing store to a distributed key/value store. See for example the
  [bitraft](https://git.mills.io/prologic/bitraft) as an example of this.

- For high performance, low latency read/write workloads where you cannot fit
  a typical hash-map into memory, but require the highest level of performance
  and predicate read latency. Bitcask ensures only 1 read/write IOPS are ever
  required for reading and writing key/value pairs.

- As a general purpose embedded key/value store where you would have used
  [BoltDB](https://github.com/boltdb/bolt),
  [LevelDB](https://github.com/syndtr/goleveldb),
  [BuntDB](https://github.com/tidwall/buntdb)
  or similar...

`bitcask` is not suited for:

- Storing billions of records
  The reason for this is the key-space is held in memory using radix tree.
  This means the more keys you have in your key space, the more memory is
  consumed. Consider using a disk-backed B-Tree like [BoltDB](https://github.com/boltdb/bolt)
  or [LevelDB](https://github.com/syndtr/goleveldb) if you intend to store a
  large quantity of key/value pairs.

> Note however that storing large amounts of data in terms of value(s) is
> totally fine. In other wise thousands to millions of keys with large values
> will work just fine.

- Write intensive workloads. Due to the [Bitcask design](https://riak.com/assets/bitcask-intro.pdf?source=post_page---------------------------)
  heavy write workloads that lots of key/value pairs will over time cause
  problems like "Too many open files" (#193) errors to occur. This can be mitigated by
  periodically compacting the data files by issuing a `.Merge()` operation however
  if key/value pairs do not change or are never deleted, as-in only new key/value
  pairs are ever written this will have no effect. Eventually you will run out
  of file descriptors!

> You should consider your read/write workloads carefully and ensure you set
> appropriate file descriptor limits with `ulimit -n` that suit your needs.

## Development

```sh
$ git clone https://go.mills.io/bitcask.git
$ make
```

## Install

```sh
$ go get go.mills.io/bitcask
```

## Usage (library)

Install the package into your project:

```sh
$ go get go.mills.io/bitcask
```

```go
package main

import (
	"log"
	"go.mills.io/bitcask"
)

func main() {
    db, _ := bitcask.Open("/tmp/db")
    defer db.Close()
    db.Put([]byte("Hello"), []byte("World"))
    val, _ := db.Get([]byte("Hello"))
    log.Printf(string(val))
}
```

See the [GoDoc](https://godoc.org/go.mills.io/bitcask) for further
documentation and other examples.

## Usage (tool)

```sh
$ bitcask -p /tmp/db set Hello World
$ bitcask -p /tmp/db get Hello
World
```

## Usage (server)

There is also a builtin very  simple Redis-compatible server called `bitcaskd`:

```sh
$ ./bitcaskd ./tmp
INFO[0000] starting bitcaskd v0.0.7@146f777              bind=":6379" path=./tmp
```

Example session:

```sh
$ telnet localhost 6379
Trying ::1...
Connected to localhost.
Escape character is '^]'.
SET foo bar
+OK
GET foo
$3
bar
DEL foo
:1
GET foo
$-1
PING
+PONG
QUIT
+OK
Connection closed by foreign host.
```

## Docker

You can also use the [Bitcask Docker Image](https://cloud.docker.com/u/prologic/repository/docker/prologic/bitcask):

```sh
$ docker pull prologic/bitcask
$ docker run -d -p 6379:6379 prologic/bitcask
```

## Performance

Benchmarks run on a 11" MacBook with a 1.4Ghz Intel Core i7:

```sh
$ make bench
...
goos: darwin
goarch: amd64
pkg: go.mills.io/bitcask

BenchmarkGet/128B-4         	  316515	      3263 ns/op	  39.22 MB/s	     160 B/op	       1 allocs/op
BenchmarkGet/256B-4         	  382551	      3204 ns/op	  79.90 MB/s	     288 B/op	       1 allocs/op
BenchmarkGet/512B-4         	  357216	      3835 ns/op	 133.51 MB/s	     576 B/op	       1 allocs/op
BenchmarkGet/1K-4           	  274958	      4429 ns/op	 231.20 MB/s	    1152 B/op	       1 allocs/op
BenchmarkGet/2K-4           	  227764	      5013 ns/op	 408.55 MB/s	    2304 B/op	       1 allocs/op
BenchmarkGet/4K-4           	  187557	      5534 ns/op	 740.15 MB/s	    4864 B/op	       1 allocs/op
BenchmarkGet/8K-4           	  153546	      7652 ns/op	1070.56 MB/s	    9472 B/op	       1 allocs/op
BenchmarkGet/16K-4          	  115549	     10272 ns/op	1594.95 MB/s	   18432 B/op	       1 allocs/op
BenchmarkGet/32K-4          	   69592	     16405 ns/op	1997.39 MB/s	   40960 B/op	       1 allocs/op

BenchmarkPut/128BNoSync-4   	  123519	     11094 ns/op	  11.54 MB/s	      49 B/op	       2 allocs/op
BenchmarkPut/256BNoSync-4   	   84662	     13398 ns/op	  19.11 MB/s	      50 B/op	       2 allocs/op
BenchmarkPut/1KNoSync-4     	   46345	     24855 ns/op	  41.20 MB/s	      58 B/op	       2 allocs/op
BenchmarkPut/2KNoSync-4     	   28820	     43817 ns/op	  46.74 MB/s	      68 B/op	       2 allocs/op
BenchmarkPut/4KNoSync-4     	   13976	     90059 ns/op	  45.48 MB/s	      89 B/op	       2 allocs/op
BenchmarkPut/8KNoSync-4     	    7852	    155101 ns/op	  52.82 MB/s	     130 B/op	       2 allocs/op
BenchmarkPut/16KNoSync-4    	    4848	    238113 ns/op	  68.81 MB/s	     226 B/op	       2 allocs/op
BenchmarkPut/32KNoSync-4    	    2564	    391483 ns/op	  83.70 MB/s	     377 B/op	       3 allocs/op

BenchmarkPut/128BSync-4     	     260	   4611273 ns/op	   0.03 MB/s	      48 B/op	       2 allocs/op
BenchmarkPut/256BSync-4     	     265	   4665506 ns/op	   0.05 MB/s	      48 B/op	       2 allocs/op
BenchmarkPut/1KSync-4       	     256	   4757334 ns/op	   0.22 MB/s	      48 B/op	       2 allocs/op
BenchmarkPut/2KSync-4       	     255	   4996788 ns/op	   0.41 MB/s	      92 B/op	       2 allocs/op
BenchmarkPut/4KSync-4       	     222	   5136481 ns/op	   0.80 MB/s	      98 B/op	       2 allocs/op
BenchmarkPut/8KSync-4       	     223	   5530824 ns/op	   1.48 MB/s	      99 B/op	       2 allocs/op
BenchmarkPut/16KSync-4      	     213	   5717880 ns/op	   2.87 MB/s	     202 B/op	       2 allocs/op
BenchmarkPut/32KSync-4      	     211	   5835948 ns/op	   5.61 MB/s	     355 B/op	       3 allocs/op

BenchmarkScan-4             	  568696	      2036 ns/op	     392 B/op	      33 allocs/op
PASS
```

For 128B values:

* ~300,000 reads/sec
* ~90,000 writes/sec
* ~490,000 scans/sec

The full benchmark above shows linear performance as you increase key/value sizes.

## Support

Support the ongoing development of Bitcask!

**Sponsor**

- Become a [Sponsor](https://www.patreon.com/prologic)

## Contributors

Thank you to all those that have contributed to this project, battle-tested it,
used it in their own projects or products, fixed bugs, improved performance
and even fix tiny typos in documentation! Thank you and keep contributing!

You can find an [AUTHORS](/AUTHORS) file where we keep a list of contributors
to the project. If you contribute a PR please consider adding your name there.

## Related Projects

- [bitraft](https://git.mills.io/prologic/bitraft) -- A Distributed Key/Value store (_using Raft_) with a Redis compatible protocol.
- [bitcaskfs](https://go.mills.io/bitcaskfs) -- A FUSE file system for mounting a Bitcask database.
- [bitcask-bench](https://go.mills.io/bitcask-bench) -- A benchmarking tool comparing Bitcask and several other Go key/value libraries.

## License

bitcask is licensed under the term of the [MIT License](https://go.mills.io/bitcask/blob/master/LICENSE)
