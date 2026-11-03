export GOWORK := off
export GOTMPDIR := $(CURDIR)/.tools/go-build

.PHONY: generate check-generated test lint-code-fast package-development test-package-development

generate:
	bash scripts/generate.sh

check-generated:
	bash scripts/generate.sh --check

test:
	mkdir -p "$(GOTMPDIR)"
	go test -tags test_dep ./...
	npm run check
	npm test

lint-code-fast:
	mkdir -p "$(GOTMPDIR)"
	go vet -tags test_dep ./...

package-development:
	python3 scripts/package-development.py

test-package-development:
	python3 scripts/test-package-development.py
