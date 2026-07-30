# Feature: VS Expression Translator — Date Functions

Translates Exasol date/time function and EXTRACT nodes per dialect, admitting to the DataFusion
dialect only those functions whose DataFusion semantics match Exasol's.

## Background

* This delta SUPERSEDES the preceding Background bullet "Only date/time functions whose DataFusion semantics match Exasol's are translated. Functions that depend on Exasol session state or whose DataFusion equivalent diverges in result are left unsupported (the node returns an error in raising mode, `None` in the safe variants), so the adapter omits them and Exasol post-processes them as a correctness backstop. This parity gate governs the DataFusion dialect, which decides what the node-local scan evaluates. It does not govern the Exasol dialect, which renders the name Exasol sent and so has nothing to reach parity with." The parity gate itself is unchanged and still correct. The claim that the adapter can omit an unsupported node and let Exasol post-process it holds ONLY while the corresponding capability is unadvertised — an unadvertised function is never delegated, so Exasol keeps it. Once the capability IS advertised, Exasol delegates the node and re-applies nothing; the caller must then apply it itself. See `vs-adapter/pushdown-planning-capability-extensions` for the safe direction of that trade and `vs-adapter/pushdown-declined-filter-self-apply` for what a caller does with a delegated node it cannot render.
* An ARITY the DataFusion dialect refuses is the same case as a name it refuses, and it is reachable under an advertised capability: the Exasol dialect renders a declared verbatim-call name at any arity, while the DataFusion dialect checks each name's arity in its own arm. A pushed call whose arity no DataFusion arm accepts therefore declines in the DataFusion dialect and renders in the Exasol dialect, which is exactly the shape the caller self-applies.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: A refused argument count declines for DataFusion and renders for Exasol

* *GIVEN* a `function_scalar` node whose name is a declared verbatim-call date/time function and whose argument count exceeds what the DataFusion dialect's per-name arm accepts — for example `SECOND(<datetime>, <precision>)`, whose Exasol signature takes an optional precision and whose DataFusion arm accepts exactly one argument
* *WHEN* the node is rendered in each dialect
* *THEN* the DataFusion dialect SHALL return an error in raising mode and `None` in the safe variants, because no DataFusion arm expresses that call
* *AND* the Exasol dialect SHALL render the call verbatim — the name, argument order, and argument count Exasol sent — because Exasol's own compiler emitted it
* *AND* the caller SHALL treat that asymmetry as a decline to self-apply, not as an omission, because the function's capability is advertised and Exasol therefore delegated the call
<!-- /DELTA:NEW -->

