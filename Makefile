devsetup:
	go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.39.0
	go get github.com/vektra/mockery/.../
	git submodule update --remote --init --recursive

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

updatetestcases:
	git submodule update --remote --init --recursive

updatemocks:
	mockery --name=connectionManager --output=. --testonly --inpackage
	mockery --name=kvProvider --output=. --testonly --inpackage
	mockery --name=httpProvider --output=. --testonly --inpackage
	mockery --name=diagnosticsProvider --output=. --testonly --inpackage
	mockery --name=mgmtProvider --output=. --testonly --inpackage
	mockery --name=analyticsProvider --output=. --testonly --inpackage
	mockery --name=queryProvider --output=. --testonly --inpackage
	mockery --name=searchProvider --output=. --testonly --inpackage
	mockery --name=viewProvider --output=. --testonly --inpackage
	mockery --name=waitUntilReadyProvider --output=. --testonly --inpackage
	mockery --name=kvCapabilityVerifier --output=. --testonly --inpackage
	# pendingOp is manually mocked

.PHONY: all test devsetup fasttest lint cover check bench updatetestcases updatemocks
