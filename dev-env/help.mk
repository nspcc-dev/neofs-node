.PHONY: help

HELP_MAKEFILE_LIST=$(filter-out %/artifacts.mk, $(MAKEFILE_LIST))

# Show this help prompt
help:
	@echo '  Usage:'
	@echo ''
	@echo '    make <target>'
	@echo ''
	@echo '  Targets:'
	@echo ''
	@awk '/^#/{ comment = substr($$0,3) } comment && /^[a-zA-Z][a-zA-Z0-9._/-]+ ?:/{ print "   ", $$1, comment }' $(HELP_MAKEFILE_LIST) | column -t -s ':' | grep -v 'IGNORE' | sort -u
