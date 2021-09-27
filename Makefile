PROJECT_NAME := "dkafka"
PKG := "./cmd/$(PROJECT_NAME)"
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/ | grep -v _test.go)
BUILD_DIR := "./build"
BINARY_PATH := $(BUILD_DIR)/$(PROJECT_NAME)
COVERAGE_DIR := $(BUILD_DIR)
INCLUDE_EXPRESSION ?= "executed && action=='create' && account=='eosio.nft.ft' && receiver=='eosio.nft.ft'"
KEY_EXPRESSION ?= "[transaction_id]"
COMPRESSION_TYPE ?= "none"
COMPRESSION_LEVEL ?= -1
KUBECONFIG ?= ~/.kube/dev.dfuse.kube
START_BLOCK ?= 0
# Source:
#   https://about.gitlab.com/blog/2017/11/27/go-tools-and-gitlab-how-to-do-continuous-integration-like-a-boss/
#   https://gitlab.com/pantomath-io/demo-tools/-/tree/master

.PHONY: all dep build clean test cov covhtml lint

all: build

lint: ## Lint the files
	@golint -set_exit_status ${PKG_LIST}

test: ## Run unittests
	@go test -short

race: dep ## Run data race detector
	@go test -race -short .

msan: dep ## Run memory sanitizer
	@go test -msan -short .

cov: ## Generate global code coverage report
	@mkdir -p $(COVERAGE_DIR)
	@go test -covermode=count -coverprofile $(COVERAGE_DIR)/coverage.cov

covhtml: ## Generate global code coverage report in HTML
	@mkdir -p $(COVERAGE_DIR)
	@go tool cover -html=$(COVERAGE_DIR)/coverage.cov -o $(COVERAGE_DIR)/coverage.html

dep: ## Get the dependencies
	@go get -v -d ./...
	@go get -u github.com/golang/lint/golint

build: ## Build the binary file
	@go build -o $(BINARY_PATH) -v $(PKG)

clean: ## Remove previous build
	@rm -rf $(BUILD_DIR)

up: ## Launch docker compose
	@docker-compose up -d


start: build up ## start dkafka localy
	$(BINARY_PATH) publish \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--fail-on-undecodable-db-op \
		--kafka-cursor-topic="cursor" \
		--kafka-topic="io.dkafka.test" \
		--dfuse-firehose-include-expr=$(INCLUDE_EXPRESSION) \
		--event-keys-expr=$(KEY_EXPRESSION) \
		--event-type-expr="'TestNotification'" \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--start-block-num=$(START_BLOCK)

forward: ## open port forwarding on dfuse dev
	KUBECONFIG=$(KUBECONFIG) kubectl -n ultra-dev port-forward firehose-v3-0 9000 &
	KUBECONFIG=$(KUBECONFIG) kubectl -n ultra-dev port-forward svc/abicodec-v3 9001:9000 &

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
