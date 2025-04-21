CONFIG_PATH=${HOME}/.gumlog/

.PHONY: init
# create a root directory to store the generated certs
init:
	mkdir -p ${CONFIG_PATH}

# generate certs
.PHONY: gencert
gencert:
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
	test/server-csr.json | cfssljson-bare server
	mv *.pem *.csr ${CONFIG_PATH}

.PHONY: compile
compile:
	@echo "Compiling protobuf..."
	protoc api/v1/*.proto \
	--go_out=. \
	--go-grpc_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_opt=paths=source_relative \
	--proto_path=.

.PHONY: test
test:
	@echo "Running tests..."
	go test -race -v ./...