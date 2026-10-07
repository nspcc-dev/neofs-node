#!/usr/bin/env bash

echo "Running bin/tick.sh"

# Source env settings
. .env
source bin/helper.sh

# NeoFS ADM binary path and config path.
NEOFS_ADM="${NEOFS_ADM:-./vendor/neofs-adm}"
CONFIG_ADM="${CONFIG_ADM:-./neofs-adm.yml}"

${NEOFS_ADM} fschain force-new-epoch --alphabet-wallets services/ir${IR_NUMBER_OF_NODES}/alphabet \
  -r http://ir01.${LOCAL_DOMAIN}:30333 -c ${CONFIG_ADM}
