REBAR?=rebar

ERL_FLAGS=ERL_FLAGS="-config test/eunit"

all: build

build:
	$(REBAR) compile

clean:
	$(REBAR) clean

doc:
	$(REBAR) doc

test: build
	$(ERL_FLAGS) $(REBAR) eunit

.PHONY: all build clean doc test
