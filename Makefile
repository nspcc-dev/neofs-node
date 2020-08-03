#!/usr/bin/make -f
SHELL = bash

REPO ?= $(shell go list -m)
VERSION ?= "$(shell git describe --tags --dirty --always)"

HUB_IMAGE ?= nspccdev/neofs
HUB_TAG ?= "$(shell echo ${VERSION} | sed 's/^v//')"

BIN = bin
DIRS= $(BIN)

# List of binaries to build. May be automated.
CMDS = neofs-node neofs-ir
CMS = $(addprefix $(BIN)/, $(CMDS))
BINS = $(addprefix $(BIN)/, $(CMDS))

.PHONY: help all dep clean fmts fmt imports test lint docker/lint

# To build a specific binary, use it's name prefix with bin/ as a target
# For example `make bin/neofs-node` will build only storage node binary
# Just `make` will build all possible binaries
all: $(DIRS) $(BINS)

$(BINS): $(DIRS) dep
	@echo "⇒ Build $@"
	CGO_ENABLED=0 \
	GO111MODULE=on \
	go build -v -trimpath \
	-ldflags "-X ${REPO}/misc.Version=$(VERSION) -X ${REPO}/misc.Build=${BUILD}" \
	-o $@ ./cmd/$(notdir $@)

$(DIRS):
	@echo "⇒ Ensure dir: $@"
	@mkdir -p $@

# Pull go dependencies
dep:
	@printf "⇒ Tidy requirements : "
	CGO_ENABLED=0 \
	GO111MODULE=on \
	go mod tidy -v && echo OK
	@printf "⇒ Download requirements: "
	CGO_ENABLED=0 \
	GO111MODULE=on \
	go mod download && echo OK
	@printf "⇒ Install test requirements: "
	CGO_ENABLED=0 \
	GO111MODULE=on \
	go test -i ./... && echo OK

# Regenerate proto files:
protoc:
	@GOPRIVATE=github.com/nspcc-dev go mod vendor
	# Install specific version for gogo-proto
	@go list -f '{{.Path}}/...@{{.Version}}' -m github.com/gogo/protobuf | xargs go get -v
	# Install specific version for protobuf lib
	@go list -f '{{.Path}}/...@{{.Version}}' -m  github.com/golang/protobuf | xargs go get -v
	# Protoc generate
	@for f in `find . -type f -name '*.proto' -not -path './vendor/*'`; do \
		echo "⇒ Processing $$f "; \
		protoc \
			--proto_path=.:./vendor:./vendor/github.com/nspcc-dev/neofs-api-go:/usr/local/include \
			--gofast_out=plugins=grpc,paths=source_relative:. $$f; \
	done
	rm -rf vendor

# Build NeoFS Storage Node docker image
image-%:
	@echo "⇒ Build NeoFS $* docker image "
	@docker build \
		--build-arg REPO=$(REPO) \
		--build-arg VERSION=$(VERSION) \
		--rm \
		-f Dockerfile.$* \
		-t $(HUB_IMAGE)-$*:$(HUB_TAG) .

# Build all Docker images
images: image-storage image-ir

# Run all code formaters
fmts: fmt imports

# Reformat code
fmt:
	@echo "⇒ Processing gofmt check"
	@GO111MODULE=on gofmt -s -w cmd/ pkg/ misc/

# Reformat imports
imports:
	@echo "⇒ Processing goimports check"
	@GO111MODULE=on goimports -w cmd/ pkg/ misc/

# Run Unit Test with go test
test:
	@echo "⇒ Runnning go test"
	@GO111MODULE=on go test ./...

# Run linters
lint:
	@golangci-lint run

# Run linters in Docker
docker/lint:
	docker run --rm -it \
	-v `pwd`:/src \
	-u `stat -c "%u:%g" .` \
	--env HOME=/src \
	golangci/golangci-lint:v1.30 bash -c 'cd /src/ && make lint'

# Print version
version:
	@echo $(VERSION)

# Show this help prompt
help:
	@echo '  Usage:'
	@echo ''
	@echo '    make <target>'
	@echo ''
	@echo '  Targets:'
	@echo ''
	@awk '/^#/{ comment = substr($$0,3) } comment && /^[a-zA-Z][a-zA-Z0-9_-]+ ?:/{ print "   ", $$1, comment }' $(MAKEFILE_LIST) | column -t -s ':' | grep -v 'IGNORE' | sort -u

clean:
	rm -rf vendor
	rm -rf $(BIN)
