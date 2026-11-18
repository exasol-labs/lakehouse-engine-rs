# Intent: change-broadcast-join-conditions

## Interview

- Q: Plan request? → A: #664, broadcast composite keys and joins with filters in `ON`
- Q: Ship composite keys now, or wait for DataFusion statistics pruning on the struct key set? → A: Ship now, document the weaker pruning
- Q: Is the struct `IN`-list pruning gap filed upstream? → A: No issue found in apache/datafusion; accepted for now
- Q: Can an inner join nested under an outer join reach the adapter? → A: Spike on Exasol 2026.2: Exasol sends a null-supplying subtree as its own request
- Q: Which guard keeps the move correct once outer joins are advertised? → A: Path flag: skip joins on an outer join's null-supplying side
- Q: Why can't `ON o.k = c.k + 1 AND …` broadcast? → A: It can; DataFusion hash-joins expression keys
- Q: What counts as a join key? → A: Any `=` whose two sides each read one leg, different legs, expressions included
- Q: One plan or split? → A: One plan
- Q: Keyless condition (`a.k + b.k = c.k AND c.r = 'x'`) stays whole, or equality stays and filters move? → A: Equality stays, filters move
- Q: Design constraint for that rule? → A: Supple: switching the stays-rule later must be cheap

## Directions

- [D1] One planner module owns three separate tests: which conjuncts stay in a condition (join equality), the move, and the broadcast join-key test; each rule then has one owner and changes alone.
- [D2] No upstream DataFusion issue is opened for the struct key-set pruning; planning opens no issues, so it is an open question (#TBD) for a human.
- [D3] The spike's finding that Exasol splits null-supplying subtrees stays out of the specs; the guard does not rely on it, so no scenario asserts Exasol's behavior.
- [D4] The scan feature's Background bullet "the whole pushed key set prunes" stays unchanged; the changed scenario scopes it to one plain-column key, and rewriting that 15-bullet Background is out of scope.
- [D5] Each inner join's condition is split before WHERE attribution: its join equalities stay in the condition and never reach a side, and its other conjuncts become top-level WHERE conjuncts that per-side attribution handles like any other; the per-side, cross-table, and OR rules stay. ADR: yes, criterion 2 (long-lived constraint binding #663), supersedes per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution, § Constraints
- [D6] Moved conjuncts reach file pruning only through the existing WHERE pruning path, so the plan adds no deviation from the Iceberg spec's inclusive-projection rule for scan planning or Delta's data skipping.
