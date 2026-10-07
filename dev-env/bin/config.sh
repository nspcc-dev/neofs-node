#!/usr/bin/env bash

echo "Running bin/config.sh"

# Source env settings
. .env
source bin/helper.sh

# NeoFS ADM binary path and config path.
NEOFS_ADM="${NEOFS_ADM:-./vendor/neofs-adm}"
CONFIG_ADM="${CONFIG_ADM:-./neofs-adm.yml}"

# NeoFS configuration record:
# key is a string and value is a constant of [string|int|etc] type
KEY=${1}
VALUE="${2}"

[ -z "$KEY" ] && echo "Empty config key" && exit 1
[ -z "$VALUE" ] && echo "Empty config value" && exit 1

# Change config value in side chain
echo "Changing ${KEY} configuration value to ${VALUE}"

${NEOFS_ADM} fschain set-config --alphabet-wallets services/ir${IR_NUMBER_OF_NODES}/alphabet \
  -r http://ir01.${LOCAL_DOMAIN}:30333 -c ${CONFIG_ADM}  ${KEY}=${VALUE} --force

# Update epoch to apply new configuration value
./bin/tick.sh
