
<a name="v2.0.1"></a>
## [v2.0.1](https://go.mills.io/bitcask/v2/compare/v2.0.0...v2.0.1) (2023-11-12)

### Bug Fixes

* Fix v2 import path


<a name="v2.0.0"></a>
## [v2.0.0](https://go.mills.io/bitcask/v2/compare/v1.0.2...v2.0.0) (2023-11-12)

### Bug Fixes

* Fix benchmark results
* Fix install target
* Fix install target and versioning

### Features

* Add support for high-level data types (#253)

### Updates

* Update CHANGELOG for v2.0.0
* Update performance section (again)
* Update Performance section in README
* Update README


<a name="v1.0.2"></a>
## [v1.0.2](https://go.mills.io/bitcask/v2/compare/v1.0.1...v1.0.2) (2021-11-01)

### Bug Fixes

* Fix a data race in Datafile.ReadAt()
* Fix release tool

### Updates

* Update CHANGELOG for v1.0.2


<a name="v1.0.1"></a>
## [v1.0.1](https://go.mills.io/bitcask/v2/compare/v1.0.0...v1.0.1) (2021-10-31)

### Features

* Add ErrBadConfig and ErrBadMetadata as errors that consumers can check and use (#241)
* Add key prefix matching to KEYS command (#237)

### Updates

* Update CHANGELOG for v1.0.1
* Update image target


<a name="v1.0.0"></a>
## [v1.0.0](https://go.mills.io/bitcask/v2/compare/v0.3.14...v1.0.0) (2021-07-24)

### Updates

* Update CHANGELOG for v1.0.0
* Update CHANGELOG for 1.0.0
* Update README


<a name="v0.3.14"></a>
## [v0.3.14](https://go.mills.io/bitcask/v2/compare/v0.3.13...v0.3.14) (2021-07-21)

### Bug Fixes

* Fix runGC behaviour to correctly delete all expired keys (#229)
* Fix missing push event
* Fix how CI is triggered
* Fix README Go Reference badge
* Fix README badges

### Features

* Add RangeScan() support (#160)

### Updates

* Update CHANGELOG for v0.3.14


<a name="v0.3.13"></a>
## [v0.3.13](https://go.mills.io/bitcask/v2/compare/v0.3.12...v0.3.13) (2021-07-16)

### Bug Fixes

* Fix paths used for temporary recovery iles to avoid crossing devices (#223)

### Features

* Add Drone CI config

### Updates

* Update CHANGELOG for v0.3.13


<a name="v0.3.12"></a>
## [v0.3.12](https://go.mills.io/bitcask/v2/compare/v0.3.11...v0.3.12) (2021-07-13)

### Updates

* Update CHANGELOG for v0.3.12


<a name="v0.3.11"></a>
## [v0.3.11](https://go.mills.io/bitcask/v2/compare/v0.3.10...v0.3.11) (2021-07-10)

### Bug Fixes

* Fix missing go.sum entries
* Fix GoReleaser config
* Fix go.sum

### Documentation

* Document good and possibly poor use-cases of Bitcask (#199)

### Features

* Add support for keys with ttl (#177)

### Updates

* Update CHANGELOG for v0.3.11


<a name="v0.3.10"></a>
## [v0.3.10](https://go.mills.io/bitcask/v2/compare/v0.3.9...v0.3.10) (2020-12-18)

### Bug Fixes

* Fix a bug when MaxValueSize == 0 on Merge operations
* Fix link to bitcask-bench
* Fix CI (again)
* Fix CI

### Features

* Add support for unlimited key/value sizes
* Add a few more test cases for concurrent operations

### Updates

* Update CHANGELOG for v0.3.10
* Update README.md


<a name="v0.3.9"></a>
## [v0.3.9](https://go.mills.io/bitcask/v2/compare/v0.3.8...v0.3.9) (2020-11-17)

### Bug Fixes

* Fix a race condition around .Close() and .Sync()

### Updates

* Update CHANGELOG for v0.3.9


<a name="v0.3.8"></a>
## [v0.3.8](https://go.mills.io/bitcask/v2/compare/v0.3.7...v0.3.8) (2020-11-17)

### Updates

* Update CHANGELOG for v0.3.8


<a name="v0.3.7"></a>
## [v0.3.7](https://go.mills.io/bitcask/v2/compare/v0.3.6...v0.3.7) (2020-11-17)

### Updates

* Update CHANGELOG for v0.3.7


<a name="v0.3.6"></a>
## [v0.3.6](https://go.mills.io/bitcask/v2/compare/v0.3.5...v0.3.6) (2020-11-17)

### Bug Fixes

* Fix typo in labeler (#172)
* Fix builds configuration for goreleaser
* Fix (again) goreleaser config
* Fix goreleaser config and improve release notes / changelog
* Fix recoverDatafile error covering (#162)
* Fix loadIndex to be deterministic (#115)

### Features

* Add configuration options for FileMode (#183)
* Add imports and log in example code (#182)
* Add empty changelog
* Add DependaBot config
* Add DeleteAll function (#116)

### Updates

* Update CHANGELOG for v0.3.6
* Update README.md
* Update CHANGELOG for v0.3.6
* Update CHANGELOG for v0.3.6
* Update deps (#140)
* Update README.md


<a name="v0.3.5"></a>
## [v0.3.5](https://go.mills.io/bitcask/v2/compare/v0.3.4...v0.3.5) (2019-10-20)

### Bug Fixes

* Fix setup target in Makefile to install mockery correctly
* Fix glfmt/golint issues
* Fix spelling mistake in README s/Sponser/Sponsor

### Features

* Add *.db to ignore future accidental commits of a bitcask db to the repo
* Add unit test for opening bad database with corrupted/invalid datafiles (#105)

### Updates

* Update Drone CI test pipeline
* Update README.md
* Update to Go 1.13 and update README with new benchmarks (#89)
* Update README.md


<a name="v0.3.4"></a>
## [v0.3.4](https://go.mills.io/bitcask/v2/compare/v0.3.3...v0.3.4) (2019-09-02)


<a name="v0.3.3"></a>
## [v0.3.3](https://go.mills.io/bitcask/v2/compare/v0.3.2...v0.3.3) (2019-09-02)

### Bug Fixes

* Fix a bug wit the decoder passing the wrong value for the value's offset into the buffer (#77)
* Fix typo (#65)
* Fix and cleanup some unnecessary internal sub-packages and duplication

### Updates

* Update README.md
* Update README.md
* Update README.md
* Update README.md


<a name="v0.3.2"></a>
## [v0.3.2](https://go.mills.io/bitcask/v2/compare/v0.3.1...v0.3.2) (2019-08-08)

### Updates

* Update README.md
* Update README.md
* Update CONTRIBUTING.md


<a name="v0.3.1"></a>
## [v0.3.1](https://go.mills.io/bitcask/v2/compare/v0.3.0...v0.3.1) (2019-08-05)

### Updates

* Update README.md
* Update README.md
* Update README.md


<a name="v0.3.0"></a>
## [v0.3.0](https://go.mills.io/bitcask/v2/compare/v0.2.2...v0.3.0) (2019-07-29)

### Updates

* Update README.md
* Update README.md


<a name="v0.2.2"></a>
## [v0.2.2](https://go.mills.io/bitcask/v2/compare/v0.2.1...v0.2.2) (2019-07-27)


<a name="v0.2.1"></a>
## [v0.2.1](https://go.mills.io/bitcask/v2/compare/v0.2.0...v0.2.1) (2019-07-25)


<a name="v0.2.0"></a>
## [v0.2.0](https://go.mills.io/bitcask/v2/compare/v0.1.7...v0.2.0) (2019-07-25)

### Bug Fixes

* Fix issue(db file Merge issue in windows env): (#15)


<a name="v0.1.7"></a>
## [v0.1.7](https://go.mills.io/bitcask/v2/compare/v0.1.6...v0.1.7) (2019-07-19)

### Bug Fixes

* Fix mismatched key casing. (#12)
* Fix outdated README (#11)
* Fix typos in bitcask.go docs (#10)

### Updates

* Update generated protobuf code
* Update README.md


<a name="v0.1.6"></a>
## [v0.1.6](https://go.mills.io/bitcask/v2/compare/v0.1.5...v0.1.6) (2019-04-01)

### Features

* Add Development section to README documenting use of Protobuf and tooling required. #6
* Add other badges from img.shields.io


<a name="v0.1.5"></a>
## [v0.1.5](https://go.mills.io/bitcask/v2/compare/v0.1.4...v0.1.5) (2019-03-30)

### Documentation

* Document using the Docker Image

### Features

* Add Dockerfile to publish images to Docker Hub

### Updates

* Update README.md


<a name="v0.1.4"></a>
## [v0.1.4](https://go.mills.io/bitcask/v2/compare/v0.1.3...v0.1.4) (2019-03-23)


<a name="v0.1.3"></a>
## [v0.1.3](https://go.mills.io/bitcask/v2/compare/v0.1.2...v0.1.3) (2019-03-23)


<a name="v0.1.2"></a>
## [v0.1.2](https://go.mills.io/bitcask/v2/compare/v0.1.1...v0.1.2) (2019-03-22)


<a name="v0.1.1"></a>
## [v0.1.1](https://go.mills.io/bitcask/v2/compare/v0.1.0...v0.1.1) (2019-03-22)


<a name="v0.1.0"></a>
## v0.1.0 (2019-03-22)

### Bug Fixes

* Fix a race condition + Use my fork of trie
* Fix concurrent read bug
* Fix concurrent write bug with multiple goroutines writing to the to the active datafile
* Fix usage output of bitcaskd

### Features

* Add docs for bitcask
* Add docs for options
* Add KeYS command to server (bitraftd)
* Add Len() to exported API (extended API)
* Add Keys() to exported API (extended API)
* Add EXISTS command to server (bitraftd)
* Add Has() to exported API (extended API)
* Add MergeOpen test case
* Add bitcaskd to install target
* Add CRC Checksum checks on reading values back
* Add prefix scan for keys using a Trie
* Add a simple Redis compatible server daemon (bitcaskd)
* Add flock on database Open()/Close() to prevent multiple concurrent processes write access. Fixes #2

### Updates

* Update README.md
* Update README.md
* Update README.md
* Update README.md
* Update README.md

