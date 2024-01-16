.PHONY: build test

all: build

test:
	@qlot exec ros -Q run -e '(asdf:test-system :1brc)' --quit

run: build
	bin/1brc 2>/dev/null

repl:
	@qlot exec ros -Q run

build: deps
	@qlot exec sbcl --eval '(ql:quickload :1brc)' --eval '(asdf:make :1brc)' --quit
	#@qlot exec ros -Q run -e '(ql:quickload :1brc)' -e '(asdf:make :1brc)' --quit

deps:
	@qlot install
