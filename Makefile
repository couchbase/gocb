devsetup:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
	go install github.com/vektra/mockery/v3@v3.7.0

test:
	go test ./

fasttest:
	go test -short ./

cover:
	go test -coverprofile=cover.out ./

lint:
	golangci-lint run -v

check: lint
	go test -short -cover -race ./

bench:
	go test -bench=. -run=none --disable-logger=true

updatemocks:
	mockery # Mocks configured in .mockery.yml.

performer-%:
	$(MAKE) -C internal/cmd/fit-performer $*

.PHONY: all test devsetup fasttest lint cover check bench updatetestcases updatemocks performer-%
