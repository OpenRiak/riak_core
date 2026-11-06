.PHONY: compile rel cover test dialyzer eqc

# since we are (principally) a dependency app of riak, we usually are
# built by the top-level rebar3, of a specific version, so let's find
# it in the usual location.
REBAR ?= ../riak/rebar3

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

cover:
	$(REBAR) eunit --cover
	$(REBAR) cover

test: compile
	$(REBAR) eunit

dialyzer:
	$(REBAR) dialyzer

xref:
	$(REBAR) xref

eqc:
	$(REBAR) as test eqc --testing_budget 120
	$(REBAR) as eqc eunit

check: test dialyzer xref
