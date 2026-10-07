set -eo pipefail

die() {
	echo "$(caller 0):" "$*" >&2
	exit 1
}
