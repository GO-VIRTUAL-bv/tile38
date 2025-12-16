all: tile38-server tile38-cli tile38-benchmark tile38-luamemtest

.PHONY: tile38-server
tile38-server:
	@./scripts/build.sh tile38-server

.PHONY: tile38-cli
tile38-cli:
	@./scripts/build.sh tile38-cli

.PHONY: tile38-benchmark
tile38-benchmark:
	@./scripts/build.sh tile38-benchmark

.PHONY: tile38-luamemtest
tile38-luamemtest:
	@./scripts/build.sh tile38-luamemtest

test: all
	@./scripts/test.sh


package:
	@rm -rf packages/
	@scripts/package.sh Windows windows amd64
	@scripts/package.sh Mac     darwin  amd64
	@scripts/package.sh Linux   linux   amd64
	@scripts/package.sh ARM     linux   arm
	@scripts/package.sh ARM64   linux   arm64

clean:
	rm -rf tile38-server tile38-cli tile38-benchmark tile38-luamemtest 

distclean: clean
	rm -rf packages/

install: all
	cp tile38-server /usr/local/bin
	cp tile38-cli /usr/local/bin
	cp tile38-benchmark /usr/local/bin

uninstall: 
	rm -f /usr/local/bin/tile38-server
	rm -f /usr/local/bin/tile38-cli
	rm -f /usr/local/bin/tile38-benchmark

docker: distclean test package
	@./scripts/docker.sh
