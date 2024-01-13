.PHONY: build

all: build

run: build
	bin/1brc 2>/dev/null

repl:
	@qlot exec ros -Q run

build:
	@qlot exec ros -Q run -e '(ql:quickload :1brc)' -e '(asdf:make :1brc)' --quit
