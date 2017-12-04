BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
COMMIT=$(shell git rev-parse HEAD)
LDFLAGS = -ldflags "-X main.gitBranch=$(BRANCH) -X main.gitCommit=$(COMMIT)"

all: clean slave

slave:
	GOOS=linux GOARCH=amd64 godep go build $(LDFLAGS) -o mysql-bridge-channel .

clean:
	go clean ./
