# Download neo-go to deploy tests environment

# Download neo-go binaries
get.neo-go: NEO_GO_DEST=./vendor
get.neo-go: NEO_GO_BIN=neo-go
get.neo-go:
	@mkdir -p ${NEO_GO_DEST}

ifeq (${NEO_GO_PATH},)
	@echo "⇒ Download compiled neo-go from ${NEO_GO_URL}"
	@curl -sSL ${NEO_GO_URL} -o ${NEO_GO_DEST}/${NEO_GO_BIN}
else
	@echo "⇒ Copy compiled neo-go from ${NEO_GO_PATH}"
	@cp -r ${NEO_GO_PATH} ${NEO_GO_DEST}
endif
	@chmod +x ${NEO_GO_DEST}/${NEO_GO_BIN}
