#!/bin/bash

source bin/helper.sh

WORKDIR=$(dirname "$0")
LOCAL_DOMAIN="$1"
CERT="${WORKDIR}/s04tls.crt"
KEY="${WORKDIR}/s04tls.key"

[[ -f ${CERT} ]] ||
	openssl req \
		-new \
		-newkey rsa:4096 \
		-x509 \
		-sha256 \
		-days 365 \
		-nodes \
		-subj "/C=RU/ST=SPB/L=St.Petersburg/O=NSPCC/OU=NSPCC/CN=s04.${LOCAL_DOMAIN}" \
		-keyout "${KEY}" \
		-out "${CERT}" \
		-addext "subjectAltName=DNS:s04.${LOCAL_DOMAIN}"  &> /dev/null ||
	die "Failed to generate SSL certificate for s04"
