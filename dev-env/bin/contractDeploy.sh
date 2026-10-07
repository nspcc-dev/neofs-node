#!/usr/bin/env bash

echo "Running bin/contractDeploy.sh"

# Source env settings
. .env
. services/ir${IR_NUMBER_OF_NODES}/.ir.env
source bin/helper.sh

# NeoGo binary path.
NEOGO="${NEOGO:-./vendor/neo-go}"

CONFIG="${CONFIG:-./wallets/config.yml}"

CONTRACT_ADDR=$(${NEOGO} util convert "${NEOFS_IR_MAINNET_CONTRACTS_NEOFS}" \
	| grep 'LE ScriptHash to Address' \
	| awk '{print $5}' \
	| grep -oP "[A-z0-9]+" \
	|| die "Cannot parse contract address: ${NEOFS_IR_MAINNET_CONTRACTS_NEOFS}")

keys=$(${NEOGO} wallet dump-keys -w services/ir${IR_NUMBER_OF_NODES}/alphabet/az.json \
  | awk -v RS=':' 'END {print $0}' \
  | grep -Eo '[0-9a-z]{66}' \
  | tr '\n' ' ' \
  || die "Cannot parse dump keys")
  
echo "${keys}"

${NEOGO} contract deploy \
  --wallet-config ${CONFIG} \
  --in vendor/contracts/neofs/contract.nef \
  --manifest vendor/contracts/neofs/manifest.json --force --await \
  -r http://main-chain.${LOCAL_DOMAIN}:30333 \
  [ true ffffffffffffffffffffffffffffffffffffffff \
  [ ${keys} ] \
  [ InnerRingCandidateFee 10000000000 WithdrawFee 100000000 ] ]
  
