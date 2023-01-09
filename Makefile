.PHONY: deps mocks test test-cover
.SILENT: test test-cover

BINARY_NAME = crimson
TESTABLE_PACKAGES = $(shell go list ./... | grep -v /mocks/)

deps:
	go mod tidy
	go mod vendor

lint: deps
	golangci-lint run -v

mocks:
	rm -rf pkg/mocks/*
	mockery --all --dir=pkg --output=pkg/mocks --keeptree

test:
	go test ${TESTABLE_PACKAGES} -count=1 -cover 

test-cover:
	go test ${TESTABLE_PACKAGES} -count=1 -cover -coverprofile=coverage.out
	go tool cover -html=coverage.out