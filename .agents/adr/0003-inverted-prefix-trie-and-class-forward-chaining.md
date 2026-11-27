# ADR-0003: Inverted Prefix-Trie Graph Storage and Class-Based Forward Chaining

## Context
In our streaming measurement graph and calculation router, observations must be classified, stored in working memory, and evaluated against multi-variable calculation templates.

Earlier iterations suffered from two architectural constraints:
1. **Flat Map Scans**: Working memory stored observations in flat maps or global indices, requiring full scans to resolve slot candidates and making entity-level cache invalidation an $O(N)$ filter operation.
2. **Instance-Coupled Calculations**: Calculations originally depended on static `InstanceID`s (e.g. `C4220-I2`), coupling pure mathematical formulas to physical device/protocol identifiers rather than domain classes.

## Decision (Y-Statement)
In the context of streaming reactive calculations,
facing dynamic sensor instance variations and the need for localized entity working memory,
we decided for an **Inverted Prefix-Trie Graph (`[BU, EntityType, EntityID, PrimaryCode, ClassID, InstanceID, ...Characteristics]`)** coupled with **Class-Based Slot Selectors** and **Natural Calculation Feedback**,
to achieve $O(\text{depth})$ path discrimination, $O(1)$ entity subtree invalidation, and declarative decoupling of calculations from ingestion instances,
accepting that trie segment traversal adds minor structural allocation overhead compared to flat pointer arrays.

## Hierarchy Geometry
```
[Root]
  └── [BusinessUnit] (e.g. "Agronomy")
        └── [EntityType] (e.g. "PLOT")
              └── [EntityID] (e.g. "plot_101")  <--- Invalidation Subtree Pruning Boundary
                    └── [PrimaryCode] (e.g. "GRAIN_YIELD")
                          └── [ClassID] (e.g. "CLASS_GRAIN_YIELD")
                                └── [InstanceID] (e.g. "COMBINE_A")
                                      └── [Characteristics...]
```

## Revisit Trigger
Revisit this architecture if the trie segment decomposition complicates code readability more than it illuminates domain traversal and cache invalidation.
