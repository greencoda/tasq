.PHONY: deps lint mocks test test-cover
.SILENT: test test-cover

TESTABLE_PACKAGES = $(shell go list ./... | grep -v ./mocks)

deps:
	go mod tidy
	go mod vendor

install-tools:
	go install github.com/vektra/mockery/v3@v3.5.1

lint: deps
	golangci-lint run -v

mock:
	rm -rf mocks/*
	mockery

test:
	go test ${TESTABLE_PACKAGES} -count=1 -cover 

test-cover:
	go test ${TESTABLE_PACKAGES} -count=1 -cover -coverprofile=coverage.out
	go tool cover -html=coverage.out
