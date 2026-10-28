//! Local Docker E2E suite. Every test FAILS (never skips) when the stack is unreachable.

#[path = "../common/mod.rs"]
mod common;
mod fixtures;

mod assume_role;
mod capability;
mod complex_type;
mod count_distinct;
mod credential_exposure;
mod direct_storage;
mod emit_declaration;
mod harness_row_cap;
mod int96_timestamp;
mod join;
mod non_ascii_identifier;
mod positional_deletes;
mod property_validation;
mod proxy;
mod pruning;
mod refresh;
mod scan;
mod timestamp_precision;
mod two_entry_points;
mod type_relaxation;
mod version_udf;
