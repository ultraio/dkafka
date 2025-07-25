PROJECT_NAME := "dkafka"
PKG := "./cmd/$(PROJECT_NAME)"
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/ | grep -v _test.go)
BUILD_DIR := "./build"
BINARY_PATH := $(BUILD_DIR)/$(PROJECT_NAME)
COVERAGE_DIR := $(BUILD_DIR)
ENV ?= prod-testnet
KUBECONFIG ?= ~/.kube/dfuse.$(ENV).kube
CODEC ?= "avro"
TOPIC ?= "io.dkafka.test"
CAPTURE ?= true
BATCH_MODE ?=true
SKIP_DBOPS ?= false

MESSAGE_TYPE ?= '{"create" : "EosioNftFtCreatedNotification","update" : "EosioNftFtUpdatedNotification","issue" : "EosioNftFtIssuedNotification"}[action]'
KEY_EXPRESSION ?= '"action"=="create" ? [data.create.memo] : [transaction_id]'
INCLUDE_EXPRESSION ?= 'executed && (action=="create" || action=="update" || action=="issue") && account=="eosio.nft.ft" && receiver=="eosio.nft.ft"'
# INCLUDE_EXPRESSION ?= 'executed && action=="create" && account=="eosio.nft.ft" && receiver=="eosio.nft.ft"'
# INCLUDE_EXPRESSION ?= 'executed && (action=="create" || action=="issue") && account=="eosio.nft.ft" && receiver=="eosio.nft.ft"'
# ACTIONS_EXPRESSION ?= '{"create":[{"key":"transaction_id", "type":"TestType"}]}'
# ACTIONS_EXPRESSION ?= '{"create":[{"filter": ["factory.a"], "key":"transaction_id", "type":"NftFtCreatedNotification"}]}'
# ACTIONS_EXPRESSION ?= '{"create":[{"filter": ["factory.a"], "key":"string(db_ops[0].new_json.id)", "type":"TestType"}]}'
# ACTIONS_EXPRESSION ?= '{"create":[{"filter": ["insert:factory.a"], "key":"string(db_ops[0].new_json.id)", "type":"NftFtCreatedNotification"}]}'
# ACTIONS_EXPRESSION ?= '{"create":[{"first": "insert:factory.a", "key":"string(db_ops[0].new_json.id)", "type":"NftFtCreatedNotification"}], "issue":[{"filter": "update:factory.a", "split": true, "key":"string(db_ops[0].new_json.id)", "type":"NftFtUpdatedNotification"}]}'

# stream-act 
STREAM_ACT_INCLUDE_EXPRESSION := 'executed && action=="transfer" && account=="eosio.token" && receiver=="eosio.token"'
STREAM_ACT_ACTIONS_EXPRESSION ?= '{"create":[{"key":"transaction_id", "type":"NftFtCreatedNotification"}],"issue":[{"key":"transaction_id", "type":"NftFtIssuedNotification"}]}'
STREAM_ACT_START_BLOCK ?= 49608000
# CDC
# CDC_TABLES_ACCOUNT ?= 'eosio.nft.ft'
## CDC TABLES
# CDC_TABLES_ACCOUNT ?= 'eosio.token'
# CDC_TABLES_TABLE_NAMES ?= 'accounts:s+k'
CDC_START_BLOCK ?= 135283642
CDC_ACCOUNT ?= eosio.nft.ft

CDC_TABLES_START_BLOCK ?= $(CDC_START_BLOCK)
CDC_TABLES_STOP_BLOCK := $(CDC_START_BLOCK)
CDC_TABLES_ACCOUNT ?= $(CDC_ACCOUNT)
CDC_TABLES_TABLE_NAMES ?= *:s+k
## CDC ACTIONS
CDC_ACTIONS_START_BLOCK ?= $(CDC_START_BLOCK)
CDC_ACTIONS_STOP_BLOCK := $(CDC_START_BLOCK)

CDC_ACTIONS_ACCOUNT ?= $(CDC_ACCOUNT)
CDC_ACTIONS_EXPRESSION ?= {"*":"transaction_id"}
##

COMPRESSION_TYPE ?= "snappy"
COMPRESSION_LEVEL ?= -1
MESSAGE_MAX_SIZE ?= 10000000
# create
# START_BLOCK ?= 37562000
# issue
START_BLOCK ?= $(CDC_TABLES_START_BLOCK)

# START_BLOCK ?= 30080000
STOP_BLOCK ?= 3994800
# Source:
#   https://about.gitlab.com/blog/2017/11/27/go-tools-and-gitlab-how-to-do-continuous-integration-like-a-boss/
#   https://gitlab.com/pantomath-io/demo-tools/-/tree/master

.PHONY: all dep build clean test cov covhtml lint

all: build

lint: ## Lint the files
	@golint -set_exit_status ${PKG_LIST}

test: ## Run unittests
	@go test -short ./...

race: dep ## Run data race detector
	@go test -race -short .

msan: dep ## Run memory sanitizer
	@go test -msan -short .

cov: ## Generate global code coverage report
	@mkdir -p $(COVERAGE_DIR)
	@go test -covermode=count -coverprofile $(COVERAGE_DIR)/coverage.cov

covhtml: cov ## Generate global code coverage report in HTML
	@mkdir -p $(COVERAGE_DIR)
	@go tool cover -html=$(COVERAGE_DIR)/coverage.cov -o $(COVERAGE_DIR)/coverage.html

dep: ## Get the dependencies
	@go get -v -d ./...
	@go get -u github.com/golang/lint/golint

build: ## Build the binary file
	@go build -o $(BINARY_PATH) -v $(PKG)

clean: ## Remove previous build
	@rm -rf $(BUILD_DIR)

bench: ## Run benchmark and save result in new.txt
	@go test -bench=adapter -benchmem -run="^$$" -count 7 -cpu 4 | tee new.txt
	@benchstat new.txt

bench-compare: bench ## Compare previous benchmark with new one
	@benchstat old.txt new.txt

bench-save: ## Save last benchmark as the new reference
	@mv new.txt old.txt

up: ## Launch docker compose
	@docker-compose up -d

down: ## Stop docker compose
	@docker-compose down

stream: ## stream expression based localy
	$(BINARY_PATH) publish \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--fail-on-undecodable-db-op \
		--kafka-cursor-topic="cursor" \
		--kafka-topic=$(TOPIC) \
		--dfuse-firehose-include-expr=$(INCLUDE_EXPRESSION) \
		--event-keys-expr=$(KEY_EXPRESSION) \
		--event-type-expr=$(MESSAGE_TYPE) \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--start-block-num=$(START_BLOCK) \
		--kafka-message-max-bytes=$(MESSAGE_MAX_SIZE)

cdc-tables-mig: ## CDC stream on tables
	$(BINARY_PATH) cdc tables \
		--capture=$(CAPTURE) \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--kafka-cursor-topic="cursor" \
		--kafka-topic=$(TOPIC) \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--kafka-message-max-bytes=$(MESSAGE_MAX_SIZE) \
		--start-block-num=$(CDC_TABLES_START_BLOCK) \
		--codec=$(CODEC) \
		--table-name='$(CDC_TABLES_TABLE_NAMES)' '$(CDC_TABLES_ACCOUNT)'

cdc-tables: ## CDC stream on tables
	$(BINARY_PATH) cdc tables \
		--capture=$(CAPTURE) \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--kafka-topic=$(TOPIC) \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--kafka-message-max-bytes=$(MESSAGE_MAX_SIZE) \
		--start-block-num=$(CDC_TABLES_START_BLOCK) \
		--stop-block-num=$(CDC_TABLES_STOP_BLOCK) \
		--batch-mode=$(BATCH_MODE) \
		--codec=$(CODEC) \
		--table-name='$(CDC_TABLES_TABLE_NAMES)' '$(CDC_TABLES_ACCOUNT)'

cdc-actions: ## CDC stream on tables
	$(BINARY_PATH) cdc actions \
		--capture=$(CAPTURE) \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--kafka-topic=$(TOPIC) \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--start-block-num=$(CDC_ACTIONS_START_BLOCK) \
		--stop-block-num=$(CDC_ACTIONS_STOP_BLOCK) \
		--kafka-message-max-bytes=$(MESSAGE_MAX_SIZE) \
		--codec=$(CODEC) \
		--batch-mode=$(BATCH_MODE) \
		--skip-dbops=$(SKIP_DBOPS) \
		--actions-expr='$(CDC_ACTIONS_EXPRESSION)' '$(CDC_ACTIONS_ACCOUNT)'

cdc-transactions: build up ## CDC stream on tables
	$(BINARY_PATH) cdc transactions \
		--capture=$(CAPTURE) \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--kafka-topic=$(TOPIC) \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--start-block-num=$(CDC_START_BLOCK) \
		--kafka-message-max-bytes=$(MESSAGE_MAX_SIZE) \
		--codec=$(CODEC)

stream-act: ## stream actions based localy
	$(BINARY_PATH) publish \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--fail-on-undecodable-db-op \
		--kafka-cursor-topic="cursor" \
		--kafka-topic=$(TOPIC) \
		--dfuse-firehose-include-expr=$(STREAM_ACT_INCLUDE_EXPRESSION) \
		--actions-expr=$(STREAM_ACT_ACTIONS_EXPRESSION) \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--start-block-num=$(STREAM_ACT_START_BLOCK) \
		--kafka-message-max-bytes=$(MESSAGE_MAX_SIZE)

batch: build up ## run batch localy
	$(BINARY_PATH) publish \
		--dfuse-firehose-grpc-addr=localhost:9000 \
		--abicodec-grpc-addr=localhost:9001 \
		--fail-on-undecodable-db-op \
		--batch-mode \
		--kafka-topic=$(TOPIC) \
		--dfuse-firehose-include-expr=$(INCLUDE_EXPRESSION) \
		--event-keys-expr=$(KEY_EXPRESSION) \
		--event-type-expr=$(MESSAGE_TYPE) \
		--kafka-compression-type=$(COMPRESSION_TYPE) \
		--kafka-compression-level=$(COMPRESSION_LEVEL) \
		--start-block-num=$(START_BLOCK) \
		--stop-block-num=$(STOP_BLOCK) \
		--kafka-message-max-bytes=$(MESSAGE_MAX_SIZE)

schemas: build ## Generate schemas
	$(BINARY_PATH) cdc schemas eosio.nft.ft:./testdata/eosio.nft.ft-2.0.abi -o ./build

# schemas: ## Generate schemas
# 	@echo ${BINARY_PATH}
#     $(BINARY_PATH) cdc schemas eosio.nft.ft:./testdata/eosio.token.abi -o ./build -n io.ultra.test

forward: ## open port forwarding on dfuse dev
	KUBECONFIG=$(KUBECONFIG) kubectl -n ultra-$(ENV) port-forward firehose-v3-0 9000 &
	KUBECONFIG=$(KUBECONFIG) kubectl -n ultra-$(ENV) port-forward svc/abicodec-v3 9001:9000 &

forward-stop: ## stop port fowarding to dfuse
	@ps -aux | grep forward | awk '{ print $$2 }' | xargs kill

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

avro-tools.jar:
	 wget https://dlcdn.apache.org/avro/avro-1.12.0/java/avro-tools-1.12.0.jar -O avro-tools.jar
	 sha512sum avro-tools.jar
	 [ "$(sha512sum avro-tools-1.12.0.jar)" = "$(cat avro-tools-1.12.0.jar.sha512)" ] || \ 
	 {
	 	echo "sha512 doesn't match; removing jar file; please check!" && \
		rm avro-tools.jar
	 }


ABI_ACCOUNT := eosio.nft.ft
ABI_FILE := testdata/eosio.nft.ft-4.0.6-snapshot.abi
TEST_AVRO_PATH := build/test-avro-generation
test-avro-generation: build avro-tools.jar ## test avro generation from abi; check variables to override values like make ABI_FILE=folder/
	mkdir -p $(TEST_AVRO_PATH)/in $(TEST_AVRO_PATH)/out
	$(BINARY_PATH) cdc schemas -o $(TEST_AVRO_PATH)/in '$(ABI_ACCOUNT):$(ABI_FILE)'
	for file in $$(ls $(TEST_AVRO_PATH)/in); do java -jar avro-tools.jar compile schema "$(TEST_AVRO_PATH)/in/$$file" $(TEST_AVRO_PATH)/out/; done

docker: ## Build docker image
	@docker build --no-cache --tag "quay.io/ultraio/dkafka:latest" .