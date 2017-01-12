all:

build:
	go install -v ./...

test:
	go test -v ./...

gen:
	go generate
