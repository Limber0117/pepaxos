# Yes, a Makefile. Because I am old.

IMPORT_PATH := github.com/tarcisiocjr/pepaxos
PKGS = master server client
GO_BIN ?= go
BIN	?= $(CURDIR)/bin

all: compile

compile: | $(BIN)
	$(foreach pkg,$(PKGS),$(GO_BIN) build $(FLAGS) -o $(BIN) ${IMPORT_PATH}/$(pkg);)

test: compile
	bin/test.sh

race: FLAGS += -race
race: compile

$(BIN):
	mkdir -p $(BIN)
