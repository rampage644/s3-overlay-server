# Define the name of the executable
BINARY_NAME = overlay-server

# Define the build target
build:
	go build -o bin/${BINARY_NAME} .
	GOOS="linux" GOARCH="amd64" go build -o bin/${BINARY_NAME}-linux-amd64 .

# Define the run target
run:
	bin/${BINARY_NAME}

# Define the test target
test:
	go test

# Define the clean target
clean:
	rm -rf bin

apply:
	terraform -chdir=deploy/ apply

destroy:
	terraform -chdir=deploy/ destroy

deploy:
	ansible-playbook -i deploy/inventory.ini deploy/playbook.yml

.PHONY: build run test clean apply destroy deploy