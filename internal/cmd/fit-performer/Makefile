update-protocol:
	rm -rf proto
	mkdir -p proto
	curl --location --fail https://github.com/couchbaselabs/fit-protocol/archive/refs/heads/main.tar.gz \
		| tar -xz --strip-components=2 -C proto fit-protocol-main/operational
	sed -i '' 's|github.com/couchbaselabs/transactions-fit-performer/protocol|github.com/couchbase/gocb/v2/internal/cmd/fit-performer/protocol|g' proto/*.proto

generate:
	go generate .

lint: generate
	golangci-lint run -v

.PHONY: all lint generate update-protocol
