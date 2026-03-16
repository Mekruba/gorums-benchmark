.PHONY: proto build install-plugin

GORUMS_PLUGIN = $(shell go env GOPATH)/bin/protoc-gen-gorums
PROTO_DIR     = pbft.gorums.new/proto
BINARY        = pbft.gorums.new/pbft

proto:
	protoc \
		--go_out=$(PROTO_DIR) \
		--go_opt=paths=source_relative \
		--plugin=protoc-gen-gorums=$(GORUMS_PLUGIN) \
		--gorums_out=$(PROTO_DIR) \
		--gorums_opt=paths=source_relative \
		-I ../gorums \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/pbft.proto

build:
	go build -o $(BINARY) ./pbft.gorums.new/

install-plugin:
	cd ../gorums && go install ./cmd/protoc-gen-gorums
