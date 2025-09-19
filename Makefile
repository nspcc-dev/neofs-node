#!/usr/bin/make -f
SHELL = bash

REPO ?= $(shell go list -m)
VERSION ?= $(shell set -o pipefail; git describe --tags --dirty --match "v*" --always --abbrev=8 2>/dev/null | sed 's/^v//' || cat VERSION 2>/dev/null || echo "develop")

HUB_IMAGE ?= nspccdev/neofs
HUB_TAG ?= $(VERSION)

GO_VERSION ?= 1.25
LINT_VERSION ?= 1.55.0
ARCH = amd64

BIN = bin
RELEASE = release
DIRS = $(BIN) $(RELEASE)

# List of binaries to build.
CMDS = $(filter-out internal, $(notdir $(basename $(wildcard cmd/*))))
BINS = $(addprefix $(BIN)/, $(CMDS))

# .deb package versioning
OS_RELEASE = $(shell lsb_release -cs)
PKG_VERSION ?= $(shell echo $(VERSION) | sed "s/^v//" | \
			sed -E "s/(.*)-(g[a-fA-F0-9]{6,8})(.*)/\1\3~\2/" | \
			sed "s/-/~/")-${OS_RELEASE}

.PHONY: help all images dep clean fmts fmt imports test lint docker/lint
		prepare-release debpackage

# To build a specific binary, use it's name prefix with bin/ as a target
# For example `make bin/neofs-node` will build only storage node binary
# Just `make` will build all possible binaries
all: $(DIRS) $(BINS)

# help target
include help.mk

$(BINS): $(DIRS) dep
	@echo "⇒ Build $@"
	CGO_ENABLED=0 \
	go build -v -trimpath \
	-ldflags "-X $(REPO)/misc.Version=$(VERSION)" \
	-o $@ ./cmd/$(notdir $@)

$(DIRS):
	@echo "⇒ Ensure dir: $@"
	@mkdir -p $@

# Prepare binaries and archives for release
.ONESHELL:
prepare-release: docker/all
	@for file in `ls -1 $(BIN)/*`; do
		cp $$file $(RELEASE)/`basename $$file`-$(ARCH)
		strip $(RELEASE)/`basename $$file`-$(ARCH)
		tar -czf $(RELEASE)/`basename $$file`-$(ARCH).tar.gz $(RELEASE)/`basename $$file`-$(ARCH)
	done

# Pull go dependencies
dep:
	@printf "⇒ Download requirements: "
	CGO_ENABLED=0 \
	go mod download && echo OK
	@printf "⇒ Tidy requirements : "
	CGO_ENABLED=0 \
	go mod tidy -v && echo OK

# Regenerate proto files:
protoc:
	@GOPRIVATE=github.com/nspcc-dev go mod vendor
	# Install specific version for protobuf lib
	@go list -f '{{.Path}}/...@{{.Version}}' -m  google.golang.org/protobuf | xargs go install -v
	@GOBIN=$(abspath $(BIN)) go install -mod=mod -v github.com/nspcc-dev/neofs-api-go/v2/util/protogen
	# Protoc generate
	@for f in `find . -type f -name '*.proto' -not -path './vendor/*'`; do \
		echo "⇒ Processing $$f "; \
		protoc \
			--proto_path=.:./vendor:/usr/local/include \
			--plugin=protoc-gen-go-neofs=$(BIN)/protogen \
			--go-neofs_out=. --go-neofs_opt=paths=source_relative \
			--go_out=. --go_opt=paths=source_relative \
			--go-grpc_opt=require_unimplemented_servers=false \
			--go-grpc_out=. --go-grpc_opt=paths=source_relative $$f; \
	done
	rm -rf vendor

# Build NeoFS component's docker image
image-%:
	@echo "⇒ Build NeoFS $* docker image "
	@docker buildx build \
		--build-arg REPO=$(REPO) \
		--build-arg VERSION=$(VERSION) \
		--rm \
		-f .docker/Dockerfile.$* \
		-t $(HUB_IMAGE)-$*:$(HUB_TAG) .

# Build all Docker images
images: image-storage image-ir image-cli image-adm image-storage-testnet

# Build dirty local Docker images
dirty-images: image-dirty-storage image-dirty-ir image-dirty-cli image-dirty-adm

# Run `make %` in Golang container
docker/%:
	docker run --rm -t \
	-v `pwd`:/src \
	-w /src \
	-u "$$(id -u):$$(id -g)" \
	--env HOME=/src \
	golang:$(GO_VERSION) make $*


# Run all code formatters
fmts: fmt imports

# Reformat code
fmt:
	@echo "⇒ Processing gofmt check"
	@gofmt -s -w cmd/ pkg/ misc/

# Reformat imports
imports:
	@echo "⇒ Processing goimports check"
	@goimports -w $$(find . -type f -name '*.go' -not -path '*.pb.go')

# Run Unit Test with go test
test:
	@echo "⇒ Running go test"
	@go test ./...

.golangci.yml:
	wget -O $@ https://github.com/nspcc-dev/.github/raw/master/.golangci.yml

# Run linters
lint: .golangci.yml
	@golangci-lint --timeout=5m run

cli-gendoc: $(BIN)/neofs-cli
	@echo "⇒ Generating CLI commands documentation"
	@$< gendoc ./docs/cli-commands -d

# Run linters in Docker
docker/lint:
	docker run --rm -t \
	-v `pwd`:/src \
	-u `stat -c "%u:%g" .` \
	--env HOME=/src \
	golangci/golangci-lint:v$(LINT_VERSION) bash -c 'cd /src/ && make lint'

# Print version
version:
	@echo $(VERSION)

clean:
	rm -rf vendor
	rm -rf .cache
	rm -rf $(BIN)
	rm -rf $(RELEASE)

# Package for Debian
debpackage:
	dch --package neofs-node \
			--controlmaint \
			--newversion $(PKG_VERSION) \
			--distribution $(OS_RELEASE) \
			"Please see CHANGELOG.md for code changes for $(VERSION)"
	dpkg-buildpackage --no-sign -b

debclean:
	dh clean
