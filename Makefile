# path to store configuration files
CONFIG_PATH=${shell echo $$HOME}/.gumlog

.PHONY: init
# create a root directory to store the generated certs
init:
	mkdir -p ${CONFIG_PATH}

# generate certs with CloudFlare CFSSL
# reference: https://blog.cloudflare.com/how-to-build-your-own-public-key-infrastructure/
.PHONY: gencert
gencert: init
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=./test/ca-config.json \
		-profile=server \
		./test/server-csr.json | cfssljson -bare server

# start client certs generation. this sets up multiple client certs with different permissions (root user, and nobody)
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client
	
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client

# move certs and keys into the config path
	mv *.pem *.csr ${CONFIG_PATH}

# clean app cert files
.PHONY: cleancert
cleancert:
	@echo "Cleaning app cert files..."
	rm -rf ${CONFIG_PATH}

.PHONY: compile
compile:
	@echo "Compiling protobuf..."
	protoc api/v1/*.proto \
	--go_out=. \
	--go-grpc_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_opt=paths=source_relative \
	--proto_path=.

# copy policies into acl policy and model into config path
$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf

$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

.PHONY: test
# copy acl configs before running tests
test: $(CONFIG_PATH)/policy.csv $(CONFIG_PATH)/model.conf
	@echo "Running tests..."
	go test -race -v ./...