SHELL := /bin/bash

EXASOL_IMAGE     ?= exasol/docker-db:2025.1.16
# The docker-db image's built-in SYS default; it has no password env var.
EXASOL_SYS_PASSWORD ?= exasol

export EXASOL_IMAGE
export EXASOL_SYS_PASSWORD

LAKEHOUSE_ENGINE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# MUST match the SLC toolchain and glibc (Trixie = 2.41): the SDK fingerprint
# embeds the rustc hash, so a host-built .so is rejected by the SLC at load time.
UDF_BUILDER_IMAGE ?= rust:1.94-trixie

# Real-file target so E2E targets never run against a stale binary.
VS_SO   := target/release/liblakehouse_engine.so
VS_SRCS := $(shell find crates/lakehouse-engine/src crates/lakehouse-catalog/src crates/vs-expression/src -name '*.rs') \
           crates/lakehouse-engine/Cargo.toml \
           crates/lakehouse-catalog/Cargo.toml \
           crates/vs-expression/Cargo.toml \
           Cargo.toml \
           .cargo/config.toml \
           Cargo.lock

UDF_CARGO_VOL ?= lakehouse-engine-rs-udf-cargo-registry

# Runs as the host UID:GID so target/release/** is host-owned (root-owned
# artifacts break a host `cargo clean`). The registry volume may hold root-owned
# content, so it is chowned to the host user first.
$(VS_SO): $(VS_SRCS)
	docker run --rm -v $(UDF_CARGO_VOL):/usr/local/cargo/registry \
	  $(UDF_BUILDER_IMAGE) chown -R $(shell id -u):$(shell id -g) /usr/local/cargo/registry
	docker run --rm \
	  -v $(LAKEHOUSE_ENGINE_DIR):/build/lakehouse-engine \
	  -v $(UDF_CARGO_VOL):/usr/local/cargo/registry \
	  -w /build/lakehouse-engine \
	  --user $(shell id -u):$(shell id -g) \
	  -e HOME=/tmp \
	  $(UDF_BUILDER_IMAGE) \
	  sh -c 'cargo build --release -p lakehouse-engine \
	    && cargo install cargo-exasol-udf --version "=$(SLC_VERSION)" --locked --quiet --root target/udf-tools \
	    && PATH="$$PWD/target/udf-tools/bin:$$PATH" cargo exasol-udf validate $@'

cross-udf-build: $(VS_SO)

test:
	cargo test

# Port defaults match docker-compose.yml; bench/run.sh overrides EXASOL_HOST to
# target a remote cluster.
EXASOL_HOST      ?= localhost
LH_EXASOL_PORT   ?= 28563
LH_BUCKETFS_PORT ?= 22581
LH_MINIO_PORT    ?= 19000
LH_REST_PORT     ?= 18181

export LH_EXASOL_PORT
export LH_BUCKETFS_PORT
export LH_MINIO_PORT
export LH_REST_PORT

# E2E suites FAIL (not skip) when their stack is unavailable, and run serially
# because all tests share one VS.
test-e2e: cross-udf-build
	cargo test --features exasol-e2e --test e2e_scan_test --test e2e_capability_test --test e2e_count_distinct_test --test e2e_join_test --test e2e_positional_deletes_test --test e2e_int96_timestamp_test --test e2e_refresh_test --test e2e_non_ascii_identifier_test --test e2e_harness_row_cap_test --test e2e_type_relaxation_test --test e2e_complex_type_test --test e2e_timestamp_precision_test --test e2e_credential_exposure_test --test e2e_version_udf_test --test e2e_emit_declaration_test --test e2e_direct_storage_test -- --test-threads=1

# Requires:
#   docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait \
#     minio exasol keycloak lakekeeper-db lakekeeper-migrate lakekeeper
test-e2e-lakekeeper: cross-udf-build
	cargo test --features lakekeeper-e2e --test e2e_lakekeeper_test -- --test-threads=1

# Requires:
#   docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml \
#     -f docker-compose.lakekeeper.azure.yml up -d --wait \
#     exasol keycloak lakekeeper-db lakekeeper-migrate lakekeeper
# plus real Azure Blob Storage credentials from ./test.env or the environment.
# Sourcing and cargo MUST stay on one recipe line: each line runs in its own shell.
test-e2e-azure: cross-udf-build
	if [ -f ./test.env ]; then set -a; . ./test.env; set +a; fi; cargo test --features azure-e2e --test e2e_azure_test -- --test-threads=1

# Manual equivalent of the E2E harness's in-process `setup_e2e` SLC install.
# Set BUCKETFS_WRITE_PASS to skip extracting it from EXAConf via docker exec.
SLC_VERSION ?= $(shell sed -n 's/^exasol-udf-sdk[[:space:]]*=.*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml)
ARCH ?= x86_64
ARCH_NORMALIZED := $(if $(filter arm64,$(ARCH)),aarch64,$(ARCH))
ifeq ($(ARCH_NORMALIZED),x86_64)
SLC_ARCH_SUFFIX :=
else ifeq ($(ARCH_NORMALIZED),aarch64)
SLC_ARCH_SUFFIX := -aarch64
else
$(error ARCH must be 'x86_64' or 'aarch64'; got '$(ARCH)')
endif
SLC_RELEASE_URL ?= https://github.com/exasol-labs/language-container-rs/releases/download/v$(SLC_VERSION)/lc-rust-$(SLC_VERSION)$(SLC_ARCH_SUFFIX).tar.gz
EXASOL_CONTAINER ?= lakehouse-engine-rs-exasol-1

# Single owner of the SLC version expression; bench/run.sh consumes it.
print-slc-version:
	@echo $(SLC_VERSION)

install-slc:
	@echo "=== install-slc: downloading SLC rootfs lc-rust-$(SLC_VERSION).tar.gz ==="
	curl -fsSL "$(SLC_RELEASE_URL)" -o /tmp/lakehouse-rustslc.tar.gz
	@test -s /tmp/lakehouse-rustslc.tar.gz || (echo "ERROR: SLC tarball not downloaded"; exit 1)
	@echo "=== install-slc: extracting BucketFS write password ==="
	$(eval BFSPASS := $(shell \
	  if [ -n "$$BUCKETFS_WRITE_PASS" ]; then \
	    echo "$$BUCKETFS_WRITE_PASS"; \
	  else \
	    docker exec $(EXASOL_CONTAINER) bash -c \
	      "awk '/\[\[Bucket.*default\]\]/{f=1} f&&/WritePasswd/{print \$$3;exit}' \
	       /exa/etc/EXAConf | base64 -d" 2>/dev/null; \
	  fi))
	@test -n "$(BFSPASS)" || (echo "ERROR: could not extract BucketFS write password"; exit 1)
	@echo "=== install-slc: uploading SLC to BucketFS ==="
	curl -sf -u "w:$(BFSPASS)" \
	    -T /tmp/lakehouse-rustslc.tar.gz \
	    "https://$(EXASOL_HOST):$(LH_BUCKETFS_PORT)/default/slc/lakehouse-rustslc.tar.gz" \
	    --insecure
	@echo "=== install-slc: registering RUST language alias (clean replace) ==="
	@set -e; \
	CURRENT=$$(exapump sql \
	  "SELECT SYSTEM_VALUE FROM EXA_PARAMETERS WHERE PARAMETER_NAME='SCRIPT_LANGUAGES'" \
	  -d "exasol://sys:$(EXASOL_SYS_PASSWORD)@$(EXASOL_HOST):$(LH_EXASOL_PORT)?validateservercertificate=0" \
	  2>&1 | grep -v '^\[' | grep -v '^SYSTEM_VALUE' | grep -v '^[0-9]' | grep -v '^$$' | grep -v 'Error' | head -1); \
	RUST_DEF="RUST=localzmq+protobuf:///bfsdefault/default/slc/lakehouse-rustslc?lang=rust#buckets/bfsdefault/default/slc/lakehouse-rustslc/exaudf/exaudfclient"; \
	NEW=$$(echo "$$CURRENT $$RUST_DEF" | awk '{sep=""; for(i=1;i<=NF;i++){if($$i ~ /^RUST=/ && i<NF) continue; printf "%s%s",sep,$$i; sep=" "}}'); \
	echo "Setting SCRIPT_LANGUAGES = $$NEW"; \
	exapump sql \
	  "ALTER SYSTEM SET SCRIPT_LANGUAGES = '$$NEW'" \
	  -d "exasol://sys:$(EXASOL_SYS_PASSWORD)@$(EXASOL_HOST):$(LH_EXASOL_PORT)?validateservercertificate=0"
	@echo "=== install-slc: done ==="

# Referenced from the CREATE SCRIPT body via %udf_object.
SO_BUCKETFS_PATH := /default/udf/liblakehouse_engine.so

bucketfs-upload-so: $(VS_SO)
	@echo "=== bucketfs-upload-so: extracting BucketFS write password ==="
	$(eval BFSPASS := $(shell \
	  if [ -n "$$BUCKETFS_WRITE_PASS" ]; then \
	    echo "$$BUCKETFS_WRITE_PASS"; \
	  else \
	    docker exec $(EXASOL_CONTAINER) bash -c \
	      "awk '/\[\[Bucket.*default\]\]/{f=1} f&&/WritePasswd/{print \$$3;exit}' \
	       /exa/etc/EXAConf | base64 -d" 2>/dev/null; \
	  fi))
	@test -n "$(BFSPASS)" || (echo "ERROR: could not extract BucketFS write password"; exit 1)
	@echo "=== bucketfs-upload-so: uploading liblakehouse_engine.so ==="
	curl -sf -u "w:$(BFSPASS)" \
	    -T $(VS_SO) \
	    "https://$(EXASOL_HOST):$(LH_BUCKETFS_PORT)$(SO_BUCKETFS_PATH)" \
	    --insecure
	@echo "=== bucketfs-upload-so: done ==="

fmt:
	cargo fmt

lint:
	cargo clippy --all-targets

# MUST stay flag-identical to the `cargo llvm-cov` step in ci.yml's unit-tests
# job, which is the authority. Not a prerequisite of `test`: llvm-cov relinks
# every target and link time dominates this workspace.
# Prereqs: cargo install cargo-llvm-cov --version 0.8.7; rustup component add llvm-tools-preview
coverage:
	cargo llvm-cov --workspace --lcov --output-path lcov-unit.info

test-install:
	bash deploy/scripts/tests/install.test.sh

lint-install:
	@command -v shellcheck >/dev/null 2>&1 \
	  && shellcheck -s bash deploy/scripts/install.sh deploy/scripts/tests/install.test.sh \
	  || echo "shellcheck not installed locally — skipping (CI enforces it)"

# Not wired into CI: unlike install.sh (fetched off main by users' curl|bash
# one-liner), these deploy/ scripts have no such exposure.
test-lakekeeper-scripts:
	bash deploy/scripts/tests/lakekeeper.test.sh

# FAILS (not skips) when the stack is down. Requires:
#   docker compose -f docker-compose.yml -f docker-compose.lakekeeper.yml up -d --wait \
#     minio keycloak lakekeeper-db lakekeeper-migrate lakekeeper
test-lakekeeper-local:
	bash deploy/scripts/tests/lakekeeper-local.test.sh

lint-lakekeeper-scripts:
	@if command -v shellcheck >/dev/null 2>&1; then \
	  shellcheck -s bash deploy/scripts/lakekeeper-provision.sh deploy/scripts/lakekeeper-up.sh \
	       deploy/scripts/lakekeeper-down.sh; \
	else \
	  echo "shellcheck not installed locally — skipping (no CI job runs it for these scripts)"; \
	fi

# Reads a gitignored bench/.env (see bench/.env.example); a stray one silently
# targets a remote cluster.
bench: cross-udf-build
	./bench/run.sh

# `exasol` is included so the UDF can reach the `unitycatalog` service name over
# the docker network (extra_hosts in the overlay).
unity-up:
	docker compose -f docker-compose.yml -f docker-compose.unity.yml up -d --wait \
	  minio exasol unitycatalog
	docker compose -f docker-compose.yml up -d minio-init
	./scripts/unity/seed.sh

unity-down:
	docker compose -f docker-compose.yml -f docker-compose.unity.yml down -v

# The cargo line MUST stay flag-identical to the `Run Unity Catalog E2E suite`
# step in ci.yml's e2e-unity job, which is the authority.
test-e2e-unity: cross-udf-build
	$(MAKE) unity-up
	cargo test -p lakehouse-engine --features unity-e2e --test e2e_unity_test -- --test-threads=1

.PHONY: cross-udf-build test test-e2e test-e2e-lakekeeper test-e2e-azure install-slc print-slc-version bucketfs-upload-so fmt lint coverage bench test-install lint-install unity-up unity-down test-e2e-unity test-lakekeeper-scripts test-lakekeeper-local lint-lakekeeper-scripts
