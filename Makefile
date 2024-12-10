.PHONY: test

PATHINSTBIN = $(abspath ./bin)
export PATH := $(PATHINSTBIN):$(PATH)
SHELL := env PATH=$(PATH) $(SHELL)

DEFAULT_ARCH				:= $(shell go env GOARCH)
DEFAULT_GOOS				:= $(shell go env GOOS)
ARCH						?= $(DEFAULT_ARCH)
GOOS						?= $(DEFAULT_GOOS)
.DEFAULT_GOAL 				:= test

# Dependency versions
PROMETHEUS_VERSION = 2.47.0

GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

PROMETHEUS_TAR := prometheus-$(PROMETHEUS_VERSION).$(GOOS)-$(GOARCH).tar.gz
PROMETHEUS_URL := https://github.com/prometheus/prometheus/releases/download/v$(PROMETHEUS_VERSION)/$(PROMETHEUS_TAR)

help: ## show this help
	@echo "\nSpecify a subcommand:\n"
	@grep -hE '^[0-9a-zA-Z_-]+:.*?## .*$$' ${MAKEFILE_LIST} | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[0;36m%-20s\033[m %s\n", $$1, $$2}'
	@echo ""

clean: ## clean up
	rm -rf ./bin
test: test-prom ## run all tests

test-prom: ## run prometheus tests
	sed "s/{{ .Release.Namespace }}/dev/g"  ./charts/dps/templates/alerts.yaml |  sed 's/{{.*}}//g' >  tests/prom/alerts-modified.yaml
	promtool check rules tests/prom/alerts-modified.yaml
	promtool test rules tests/prom/rules-tests.yaml

tools-promtool: ## install promtools
	mkdir -p $(PATHINSTBIN)
	wget $(PROMETHEUS_URL) -O $(PATHINSTBIN)/$(PROMETHEUS_TAR)
	tar -xvf $(PATHINSTBIN)/$(PROMETHEUS_TAR) -C $(PATHINSTBIN)
	cp $(PATHINSTBIN)/prometheus-$(PROMETHEUS_VERSION).$(GOOS)-$(GOARCH)/promtool $(PATHINSTBIN)
	rm -rf $(PATHINSTBIN)/$(PROMETHEUS_TAR) $(PATHINSTBIN)/prometheus-$(PROMETHEUS_VERSION).$(GOOS)-$(GOARCH)

make tools: tools-promtool ## install all tools
