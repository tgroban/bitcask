.PHONY: dev build generate install image release profile bench test clean setup

CGO_ENABLED=0
VERSION=$(shell git describe --abbrev=0 --tags)
COMMIT=$(shell git rev-parse --short HEAD)
BUILD=$(shell git show -s --pretty=format:%cI)
GOCMD=go

DESTDIR=/usr/local/bin

all: dev

dev: build
	@./bitcask --version
	@./bitcaskd --version

build: clean generate
	@$(GOCMD) build \
		-tags "netgo static_build" -installsuffix netgo \
		-ldflags "-w -X $(shell go list)/internal.Version=$(VERSION) -X $(shell go list)/internal.Commit=$(COMMIT) -X $(shell go list)/internal.Build=$(BUILD)" \
		./cmd/bitcask/...
	@$(GOCMD) build \
		-tags "netgo static_build" -installsuffix netgo \
		-ldflags "-w -X $(shell go list)/internal.Version=$(VERSION) -X $(shell go list)/internal.Commit=$(COMMIT) -X $(shell go list)/internal.Build=$(BUILD)" \
		./cmd/bitcaskd/...

generate:
	@$(GOCMD) generate $(shell go list)/...

install: build
	@install -D -m 755 bitcask $(DESTDIR)/bitcask
	@install -D -m 755 bitcaskd $(DESTDIR)/bitcaskd

ifeq ($(PUBLISH), 1)
image:
	@docker build --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" -t prologic/bitcask .
	@docker push prologic/bitcask
else
image:
	@docker build --build-arg VERSION="$(VERSION)" --build-arg COMMIT="$(COMMIT)" -t prologic/bitcask .
endif

release:
	@./tools/release.sh

profile: build
	@$(GOCMD) test -cpuprofile cpu.prof -memprofile mem.prof -v -bench .

bench: build
	@$(GOCMD) test -v -run=XXX -benchmem -bench=. .

test: build
	@$(GOCMD) test -v \
		-cover -coverprofile=coverage.out -covermode=atomic \
		-coverpkg=$(shell go list) \
		-race \
		.

setup:

clean:
	@git clean -f -d -X
