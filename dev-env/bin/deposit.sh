#!/usr/bin/env bash

echo "Running bin/deposit.sh"

# Source env settings
. .env
. services/ir${IR_NUMBER_OF_NODES}/.ir.env
source bin/helper.sh

# NeoGo binary path.
NEOGO="${NEOGO:-vendor/neo-go}"
# Wallet file to use for deposit GAS from
WALLET="${WALLET:-wallets/wallet.json}"
CONFIG="${CONFIG:-wallets/config.yml}"
# How much GAS to deposit. First cli argument or 50 by default
DEPOSIT="${1:-50}"

# Internal variables
ADDR=$(jq -r .accounts[0].address < "${WALLET}" \
	|| die "Cannot get address from wallet: ${WALLET}")
CONTRACT_ADDR=$(${NEOGO} util convert "${NEOFS_IR_MAINNET_CONTRACTS_NEOFS}" \
	| grep 'LE ScriptHash to Address' \
	| awk '{print $5}' \
	| grep -oP "[A-z0-9]+" \
	|| die "Cannot parse contract address: ${NEOFS_IR_MAINNET_CONTRACTS_NEOFS}")

# Make deposit
# shellcheck disable=SC2086
${NEOGO} wallet nep17 transfer \
        --wallet-config ${CONFIG} \
	-r http://main-chain.${LOCAL_DOMAIN}:30333 \
	--from ${ADDR} --force \
	--to ${CONTRACT_ADDR} \
	--token GAS \
	--amount ${DEPOSIT} || die "Cannot transfer GAS to NeoFS contract"
