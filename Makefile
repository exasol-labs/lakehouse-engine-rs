SHELL := /bin/bash

EXASOL_IMAGE     ?= exasol/docker-db:2025.2.1
EXASOL_SYS_PASSWORD ?= exasol
EXASOL_DB_MEM_SIZE  ?= 4 GiB

export EXASOL_IMAGE
export EXASOL_SYS_PASSWORD
export EXASOL_DB_MEM_SIZE

# Absolute path of this repository root.
LAKEHOUSE_ENGINE_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# Rust builder image — MUST match the SLC glibc (Bookworm = 2.36).
# Never run `cargo build --release` on the host: Ubuntu 24.04 has glibc 2.39
# and the resulting .so fails to dlopen inside Exasol.
UDF_BUILDER_IMAGE ?= rust:1.92-bookworm

# --- UDF .so artifact --------------------------------------------------------
# Real-file target: `make` rebuilds ONLY when crate sources, manifest, or the
# workspace lock change (mtime check). E2E targets depend on it so tests never
# run against a stale binary — and an unchanged crate is a sub-second no-op.
VS_SO   := target/release/liblakehouse_engine.so
VS_SRCS := $(shell find crates/lakehouse-engine/src crates/vs-expression/src -name '*.rs') \
           crates/lakehouse-engine/Cargo.toml \
           crates/vs-expression/Cargo.toml \
           Cargo.lock

# Persistent cargo registry volume — downloads happen once, not on every docker
# run. If all crates are re-downloaded the volume was dropped; it repopulates on
# the next build.
UDF_CARGO_VOL ?= lakehouse-engine-rs-udf-cargo-registry

$(VS_SO): $(VS_SRCS)
	docker run --rm \
	  -v $(LAKEHOUSE_ENGINE_DIR):/build/lakehouse-engine \
	  -v $(UDF_CARGO_VOL):/usr/local/cargo/registry \
	  -w /build/lakehouse-engine \
	  $(UDF_BUILDER_IMAGE) \
	  cargo build --release -p lakehouse-engine

# Alias: build the .so if out of date.
cross-musl-udf-build: $(VS_SO)

test:
	cargo test

# Host ports of the dedicated lakehouse-engine compose stack. Overridable so the
# suite can always pick free ports; defaults match docker-compose.yml.
# Exasol host. Defaults to localhost (Docker stack); the live-smoke script
# overrides it to target a remote cluster. install-slc / bucketfs-upload-so use
# it so the same targets work against Docker and remote Exasol.
EXASOL_HOST      ?= localhost
LH_EXASOL_PORT   ?= 28563
LH_BUCKETFS_PORT ?= 22581
LH_MINIO_PORT    ?= 19000
LH_REST_PORT     ?= 18181

export LH_EXASOL_PORT
export LH_BUCKETFS_PORT
export LH_MINIO_PORT
export LH_REST_PORT

# E2E tests require a live Exasol + MinIO + Iceberg REST catalog stack.
# They FAIL (not skip) when the stack is unavailable. All tests share one VS,
# so the binary runs serially (--test-threads=1).
test-e2e: cross-musl-udf-build
	cargo test --features exasol-e2e --test e2e_scan_test --test e2e_capability_test -- --test-threads=1

# Install and register the Rust SLC 0.14.0 into Exasol under the RUST alias.
#
# This Exasol is the dedicated lakehouse-engine stack (the sibling strata-rs stack
# is stopped), so we register the canonical RUST alias cleanly. The Rust E2E
# harness performs the same install in-process via `setup_e2e`; this target is
# the equivalent manual / convenience path.
#
# Steps:
#   1. Download lc-rust-0.14.0.tar.gz from GitHub releases.
#   2. Upload it to BucketFS at /default/slc/lakehouse-rustslc.tar.gz.
#   3. ALTER SYSTEM SET SCRIPT_LANGUAGES = '... RUST=...' (replacing any
#      pre-existing RUST= entry).
#
# BucketFS write password is extracted at runtime from EXAConf.
# Set BUCKETFS_WRITE_PASS env var to skip the docker-exec extraction.
SLC_VERSION ?= 0.14.0
SLC_RELEASE_URL ?= https://github.com/exasol-labs/language-container-rs/releases/download/v$(SLC_VERSION)/lc-rust-$(SLC_VERSION).tar.gz
EXASOL_CONTAINER ?= lakehouse-engine-rs-exasol-1

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

# Upload the compiled .so to BucketFS.
# The .so is uploaded to /default/udf/liblakehouse_engine.so and is referenced
# from the CREATE SCRIPT body via %udf_object.
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

# Manually-invoked live smoke test against a real AWS S3 + Glue Iceberg TPC-H
# catalog. Builds the working-tree .so, then runs scripts/live-smoke.sh, which
# reads config from a gitignored .env (see .env.example). NOT part of CI —
# test-e2e stays the pipeline path.
live-smoke: cross-musl-udf-build
	./scripts/live-smoke.sh

.PHONY: cross-musl-udf-build test test-e2e install-slc bucketfs-upload-so fmt lint live-smoke
