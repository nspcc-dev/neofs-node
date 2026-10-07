# Add self-signed node TLS certificate to trusted store

prepare.storage:
	@echo "Adding self-signed TLS certs to trusted store"
	@./bin/addCert.sh
