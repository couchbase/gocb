FROM golang:1.26-trixie AS build

WORKDIR /app

RUN apt update && apt install -y protobuf-compiler

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6

ENV PATH="$PATH:$(go env GOPATH)/bin"

RUN protoc --version

COPY ./ ./

WORKDIR /app/internal/cmd/fit-performer

RUN go generate
RUN go build -o go_performer

# Multistage build to keep things small
FROM debian:trixie
WORKDIR /app

ENV LOG_LEVEL="info"

COPY --from=build /app/internal/cmd/fit-performer/go_performer .
ENTRYPOINT ["./go_performer"]
