all: build

build:
	go build ./...
.PHONY: build

test:
	go test ./...
.PHONY: test

gen:
	go run ./gen
.PHONY: gen

coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out
.PHONY: coverage

benchmark:
	go test -bench=./...
.PHONY: benchmark

