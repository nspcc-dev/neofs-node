# Get NeoFS IR artifacts (NeoFS contracts, ADM and CLI)

get.ir: get.cli get.contracts get.adm get.storage prepare.storage

# Download NeoFS CLI 
.ONESHELL:
get.cli: NEOFS_CLI_FILE=./vendor/neofs-cli
get.cli: NEOFS_CLI_PATH?=
get.cli:
	@touch services/ir${IR_NUMBER_OF_NODES}/.ir.env # https://github.com/docker/compose/issues/3560
	@mkdir -p ./vendor

ifeq (${NEOFS_CLI_PATH},)
	@echo "⇒ Download NeoFS CLI binary from ${NEOFS_CLI_URL}"
	@curl -sSL "${NEOFS_CLI_URL}" -o ${NEOFS_CLI_FILE}
	@chmod 755 ${NEOFS_CLI_FILE}
else
	@echo "⇒ Copy local binary from ${NEOFS_CLI_PATH}"
	@cp ${NEOFS_CLI_PATH} ${NEOFS_CLI_FILE}
endif

# Download NeoFS Contracts
get.contracts: NEOFS_CONTRACTS_DEST=./vendor/contracts
get.contracts: NEOFS_CONTRACTS_ARCHIVE=neofs-contracts.tar.gz
get.contracts:
	@mkdir -p ${NEOFS_CONTRACTS_DEST}

ifeq (${NEOFS_CONTRACTS_PATH},)
	@echo "⇒ Download compiled NeoFS contracts from ${NEOFS_CONTRACTS_URL}"
	@curl -sSL ${NEOFS_CONTRACTS_URL} -o ${NEOFS_CONTRACTS_ARCHIVE}
	@tar -xf ${NEOFS_CONTRACTS_ARCHIVE} -C ${NEOFS_CONTRACTS_DEST} --strip-components 1 
	@rm ${NEOFS_CONTRACTS_ARCHIVE}
else
	@echo "⇒ Copy compiled contracts from ${NEOFS_CONTRACTS_PATH}"
	@cp -r ${NEOFS_CONTRACTS_PATH}/* ${NEOFS_CONTRACTS_DEST}
endif

# Download NeoFS ADM tool 
get.adm: NEOFS_ADM_DEST=./vendor/neofs-adm
get.adm:

ifeq (${NEOFS_ADM_PATH},)
	@echo "⇒ Download NeoFS ADM binary from ${NEOFS_ADM_URL}"
	@curl -sSL ${NEOFS_ADM_URL} -o ${NEOFS_ADM_DEST}
	@chmod 755 ${NEOFS_ADM_DEST}
else
	@echo "⇒ Copy neofs-adm binary from ${NEOFS_ADM_PATH}"
	@cp ${NEOFS_ADM_PATH} ${NEOFS_ADM_DEST}
endif
