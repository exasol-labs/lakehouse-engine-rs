use super::*;

// An undeclared result has no type, so the typed pass would not read its empty string as NULL.
// CAST and CASE are typed by their own node kinds.
#[test]
fn every_function_declares_its_result_rule() {
    let undeclared: Vec<&str> = SCALAR_FUNCTIONS
        .iter()
        .filter(|function| function.result == ResultRule::Other)
        .map(|function| function.name)
        .collect();
    assert_eq!(undeclared, ["CAST", "CASE"]);
}
