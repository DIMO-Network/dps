.PHONY: test

PATHINSTBIN = $(abspath ./bin)
export PATH := $(PATHINSTBIN):$(PATH)
SHELL := env PATH=$(PATH) $(SHELL)

BIN_NAME					?= dps
DEFAULT_INSTALL_DIR			:= $(go env GOPATH)/bin
DEFAULT_ARCH				:= $(shell go env GOARCH)
DEFAULT_GOOS				:= $(shell go env GOOS)
ARCH						?= $(DEFAULT_ARCH)
GOOS						?= $(DEFAULT_GOOS)
.DEFAULT_GOAL 				:= test

# Dependency versions
PROMETHEUS_VERSION = 2.47.0

GOOS := $(shell go env GOOS)
GOARCH := $(shell go env GOARCH)

# List of supported GOOS and GOARCH
GOOS_LIST := linux darwin
GOARCH_LIST := amd64 arm64

PROMETHEUS_TAR := prometheus-$(PROMETHEUS_VERSION).$(GOOS)-$(GOARCH).tar.gz
PROMETHEUS_URL := https://github.com/prometheus/prometheus/releases/download/v$(PROMETHEUS_VERSION)/$(PROMETHEUS_TAR)

help: ## show this help
	@echo "\nSpecify a subcommand:\n"
	@grep -hE '^[0-9a-zA-Z_-]+:.*?## .*$$' ${MAKEFILE_LIST} | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[0;36m%-20s\033[m %s\n", $$1, $$2}'
	@echo ""

build:
	@CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(ARCH) \
		go build -o bin/$(BIN_NAME) ./

build-all:## Build target for all supported GOOS and GOARCH
	@for goos in $(GOOS_LIST); do \
		for goarch in $(GOARCH_LIST); do \
			echo "Building for $$goos/$$goarch..."; \
			CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch \
			go build -o bin/$(BIN_NAME)-$$goos-$$goarch ./; \
		done \
	done

clean: ## clean up
	rm -rf ./bin

install: build
	@install -d $(INSTALL_DIR)
	@rm -f $(INSTALL_DIR)/benthos
	@cp bin/* $(INSTALL_DIR)/

dep:
	@go mod tidy

lint-benthos: build  ## Run Benthos linter
	@CLICKHOUSE_HOST="" CLICKHOUSE_PORT="" CLICKHOUSE_SIGNAL_DATABASE="" CLICKHOUSE_INDEX_DATABASE=""  CLICKHOUSE_USER="" CLICKHOUSE_PASSWORD="" \
		S3_AWS_ACCESS_KEY_ID="" S3_AWS_SECRET_ACCESS_KEY="" S3_CLOUDEVENT_BUCKET="" S3_EPHEMERAL_BUCKET="" \
	dps lint -r ./charts/dps/files/resources.yaml ./charts/dps/files/config.yaml ./charts/dps/files/*

lint: lint-benthos ## Run linter for benthos config and go code
	golangci-lint version
	@golangci-lint run --timeout=30m

format:
	@golangci-lint run --fix

test-go: ## Run Go tests
	@go test ./...

test: test-go test-prom ## run all tests

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

tools-golangci-lint:
	@mkdir -p $(PATHINSTBIN)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(PATHINSTBIN) $(GOLANGCI_VERSION)

make tools: tools-promtool ## install all tools
