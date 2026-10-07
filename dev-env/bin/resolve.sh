#!/usr/bin/env bash

# Source env settings
. .env
source bin/helper.sh

# NeoGo binary path.
NEOGO="${NEOGO:-docker exec main_chain neo-go}"
# NNS contract script hash
output=$(curl -s --data '{ "id": 1, "jsonrpc": "2.0", "method": "getcontractstate", "params": [1] }' \
	"http://ir01.${LOCAL_DOMAIN}:30333/") \
	|| die "Cannot fetch NNS contract state"

NNS_ADDR=$(jq -r '.result.hash' <<< "$output") \
	|| die "Cannot parse NNS contract hash: $NNS_ADDR"

${NEOGO} contract testinvokefunction \
	-r "http://ir01.${LOCAL_DOMAIN}:30333" \
	"${NNS_ADDR}" resolve string:"${1}" int:16 \
	| jq -r '.stack[0].value | if type=="array" then .[0].value else . end' \
	| base64 -d \
	|| die "Cannot invoke 'NNS.resolve' $output"
