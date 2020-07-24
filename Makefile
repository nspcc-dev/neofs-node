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

.PHONY: help dep clean fmt

# To build a specific binary, use it's name prefix with bin/ as a target
# For example `make bin/neofs-node` will build only storage node binary
# Just `make` will build all possible binaries
all: $(DIRS) $(BINS)

$(BINS): $(DIRS) dep
	@echo "⇒ Build $@"
	GOGC=off \
	CGO_ENABLED=0 \
	go build -v -mod=vendor	-trimpath \
	-ldflags "-X ${REPO}/misc.Version=$(VERSION) -X ${REPO}/misc.Build=${BUILD}" \
	-o $@ ./cmd/$(notdir $@)

$(DIRS):
	@echo "⇒ Ensure dir: $@"
	@mkdir -p $@

# Pull go dependencies
dep:
	@printf "⇒ Ensure vendor: "
	@go mod tidy -v && echo OK || (echo fail && exit 2)
	@printf "⇒ Download requirements: "
	@go mod download && echo OK || (echo fail && exit 2)
	@printf "⇒ Store vendor locally: "
	@go mod vendor && echo OK || (echo fail && exit 2)

# Regenerate proto files:
protoc:
	@GOPRIVATE=github.com/nspcc-dev go mod tidy -v
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

# Build NeoFS Storage Node docker image
image-storage:
	@echo "⇒ Build NeoFS Storage Node docker image "
	@docker build \
		--build-arg REPO=$(REPO) \
		--build-arg VERSION=$(VERSION) \
		-f Dockerfile \
		-t $(HUB_IMAGE)-storage:$(HUB_TAG) .

# Build NeoFS Storage Node docker image
image-ir:
	@echo "⇒ Build NeoFS Inner Ring docker image "
	@docker build \
		--build-arg REPO=$(REPO) \
		--build-arg VERSION=$(VERSION) \
		-f Dockerfile.ir \
		-t $(HUB_IMAGE)-ir:$(HUB_TAG) .

# Build all Docker images
images: image-storage image-ir

# Reformat code
fmt:
	@[ ! -z `which goimports` ] || (echo "Install goimports" && exit 2)
	@for f in `find . -type f -name '*.go' -not -path './vendor/*' -not -name '*.pb.go' -prune`; do \
		echo "⇒ Processing $$f"; \
		goimports -w $$f; \
	done

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
	@awk '/^#/{ comment = substr($$0,3) } comment && /^[a-zA-Z][a-zA-Z0-9_-]+ ?:/{ print "   ", $$1, comment }' $(MAKEFILE_LIST) | column -t -s ':' | grep -v 'IGNORE' | sort | uniq

clean:
	rm -rf vendor
	rm -rf $(BIN)
