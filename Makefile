.PHONY: build

all: build

build:
	qlot exec ros -Q run -e '(ql:quickload :1brc)' -e '(asdf:make :1brc)'
