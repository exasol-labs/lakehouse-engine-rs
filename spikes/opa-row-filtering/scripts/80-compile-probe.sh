#!/usr/bin/env bash
# Compile the captured OPA ASTs into DataFusion predicates and EXECUTE them.
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SPIKE_DIR/df-probe"
cargo run --quiet --bin compile-probe | tee "$SPIKE_DIR/evidence/09-rego-to-datafusion.txt"
