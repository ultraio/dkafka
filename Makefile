PROJECT_NAME := "dkafka"
PKG := "./cmd/$(PROJECT_NAME)"
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/ | grep -v _test.go)
COVERAGE_DIR := "./"
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
	@go test -covermode=count -coverprofile $(COVERAGE_DIR)/coverage.cov

covhtml: ## Generate global code coverage report in HTML
	@go tool cover -html=$(COVERAGE_DIR)/coverage.cov -o $(COVERAGE_DIR)/coverage.html

dep: ## Get the dependencies
	@go get -v -d ./...
	@go get -u github.com/golang/lint/golint

build: ## Build the binary file
	@go build -v $(PKG)

clean: ## Remove previous build
	@rm -f $(PROJECT_NAME) coverage.cov coverage.html

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
