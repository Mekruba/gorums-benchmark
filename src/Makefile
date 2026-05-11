.PHONY: proto proto-pbft proto-bftsmart build build-pbft build-bftsmart install-plugin

GORUMS_PLUGIN = $(shell go env GOPATH)/bin/protoc-gen-gorums

PBFT_PROTO_DIR     = pbft.gorums.new/proto
PBFT_BINARY        = pbft.gorums.new/pbft

BFTSMART_PROTO_DIR = bft-smart.gorums/proto
BFTSMART_BINARY    = bft-smart.gorums/bftsmart

proto: proto-pbft proto-bftsmart

proto-pbft:
	protoc \
		--go_out=$(PBFT_PROTO_DIR) \
		--go_opt=paths=source_relative \
		--plugin=protoc-gen-gorums=$(GORUMS_PLUGIN) \
		--gorums_out=$(PBFT_PROTO_DIR) \
		--gorums_opt=paths=source_relative \
		-I ../gorums \
		-I $(PBFT_PROTO_DIR) \
		$(PBFT_PROTO_DIR)/pbft.proto

proto-bftsmart:
	protoc \
		--go_out=$(BFTSMART_PROTO_DIR) \
		--go_opt=paths=source_relative \
		--plugin=protoc-gen-gorums=$(GORUMS_PLUGIN) \
		--gorums_out=$(BFTSMART_PROTO_DIR) \
		--gorums_opt=paths=source_relative \
		-I ../gorums \
		-I $(BFTSMART_PROTO_DIR) \
		$(BFTSMART_PROTO_DIR)/bftsmart.proto

build: build-pbft build-bftsmart

build-pbft:
	go build -o $(PBFT_BINARY) ./pbft.gorums.new/

build-bftsmart:
	go build -o $(BFTSMART_BINARY) ./bft-smart.gorums/

install-plugin:
	cd ../gorums && go install ./cmd/protoc-gen-gorums
