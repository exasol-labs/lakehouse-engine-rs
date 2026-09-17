#!/usr/bin/env bash
# Item 2 + item 4: run the DataFusion planning probe and record its output.
# The probe crate is deliberately OUTSIDE the repo workspace (empty [workspace]
# table in its Cargo.toml) so the spike cannot affect the shipping build.
set -euo pipefail
SPIKE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SPIKE_DIR/df-probe"
cargo run --quiet --bin opa-df-probe | tee "$SPIKE_DIR/evidence/05-datafusion-translation.txt"
