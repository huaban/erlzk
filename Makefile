REBAR?=rebar

all: build

build:
	$(REBAR) compile

clean:
	$(REBAR) clean

doc:
	$(REBAR) doc

test: build
	$(REBAR) eunit

.PHONY: all build clean doc test
