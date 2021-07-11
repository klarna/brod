all: compile

compile:
	@rebar3 compile

test-env:
	@./scripts/setup-test-env.sh

ut:
	@rebar3 eunit -v --cover_export_name ut-$(KAFKA_VERSION)

# version check, eunit and all common tests
t: ut
	@rebar3 ct -v --cover_export_name ct-$(KAFKA_VERSION)

clean:
	@rebar3 clean
	@rm -rf _build
	@rm -rf ebin deps doc
	@rm -f pipe.testdata

hex-publish: clean
	@rebar3 hex publish
	@rebar3 hex docs

## tests that require kafka running at localhost
INTEGRATION_CTS = brod_cg_commits brod_client brod_compression brod_consumer brod_producer brod_group_subscriber brod_topic_subscriber brod

## build escript and a releas, and copy escript to release bin dir
brod-cli:
	@rebar3 as brod_cli do compile,escriptize,release
	@cp _build/brod_cli/bin/brod_cli _build/brod_cli/rel/brod/bin/
	@cp scripts/brod _build/brod_cli/rel/brod/bin/
	@cp scripts/brod.escript _build/brod_cli/rel/brod/bin/

cover:
	@rebar3 cover -v

coveralls:
	@rebar3 coveralls send

dialyzer:
	@rebar3 dialyzer
