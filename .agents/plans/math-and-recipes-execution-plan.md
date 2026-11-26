# Streaming Math & Advanced Recipe Task List

## Standards & Protocol
- **Type-First Design**: Define domain orchestrators, accumulators, and interfaces first before mechanics.
- **Table-Driven Test Geometry**: Inner `type testCase struct` with `setupMock`, input fields matching signature, `want*` returns, `wantErr`, `assertSideEffects`.
- **RFC 2119 Declarations**: Every test case name MUST use uppercase RFC 2119 contract language (`"MUST ..."`).
- **Advanced-Beginner Prose**: Clear, intuitive explanations in comments and docstrings avoiding academic jargon while maintaining mathematical fidelity.
- **Git Hygiene**: Conventional commits pushed to `main`.

---

## Task Matrix

### Part 1: Core Mathematical Operators (`operators/math/`, `operators/control/`)

- [ ] **Task 1: Bivariate Online Welford, Covariance & Pearson Correlation**
  - Create `operators/math/bivariate.go`:
    - `Pair[X, Y RealNumber]`
    - `BivariateWelford` struct with `Update(x, y float64)`, `Covariance()`, `PopulationCovariance()`, `Correlation()`, `Snapshot()`.
    - `Covariance[X, Y RealNumber](ctx, in, out)`
    - `RunningCovariance[X, Y RealNumber](ctx, in, out)`
    - `PearsonCorrelation[X, Y RealNumber](ctx, in, out)`
  - Create `operators/math/bivariate_test.go` with RFC 2119 table-driven tests and runnable `ExampleBivariateWelford`.

- [ ] **Task 2: Higher-Order Central Moments (Skewness & Kurtosis - Pébay's Recurrence)**
  - Create `operators/math/moments.go`:
    - `Moments` struct tracking $M_1, M_2, M_3, M_4$, `Update(val float64)`, `Skewness()`, `Kurtosis()`, `ExcessKurtosis()`.
    - `MomentsSnapshot`, `RunningMoments`, `MomentsStats`.
  - Create `operators/math/moments_test.go` with RFC 2119 table-driven tests and runnable `ExampleMoments`.

- [ ] **Task 3: Streaming CUSUM (Cumulative Sum Control Chart)**
  - Create `operators/control/cusum.go`:
    - `CUSUMConfig`, `CUSUMState`, `CUSUMAlert`.
    - `CUSUM[T mathops.RealNumber](ctx, cfg, in, out)`
  - Create `operators/control/cusum_test.go` with RFC 2119 table-driven tests and runnable `ExampleCUSUM`.

---

### Part 2: Advanced Domain Recipes (`examples/`)

- [ ] **Task 4: Mean First Passage Time (MFPT / Renewal Barrier Crossing)**
  - Create `examples/mfpt_example_test.go`:
    - Generic `RunningMFPT` renewal operator using `pure.Scan` + `Welford`.
    - Comprehensive RFC 2119 test function `TestMFPT`.
    - Runnable `Example_mfptRenewal` with advanced beginner narrative.

- [ ] **Task 5: Goertzel Single-Tone Frequency Detector (DSP)**
  - Create `examples/dsp_goertzel_example_test.go`:
    - Online 2nd-order IIR Goertzel filter using `pure.Scan`.
    - RFC 2119 test `TestGoertzel`.
    - Runnable `Example_dspGoertzel` detecting target tone frequency in real-time.

- [ ] **Task 6: Declarative LaTeX Formula Stream Processing (`goexeclatex` Integration)**
  - Create `examples/latex_eval_example_test.go`:
    - Emulate/integrate compiled LaTeX math expression evaluation over generic streaming records.
    - RFC 2119 test `TestLaTeXEvaluation`.
    - Runnable `Example_latexStreaming` with beginner-friendly narrative.

---

### Part 3: Documentation & Verification

- [ ] **Task 7: Documentation, Verification & Push**
  - Update `README.md` operator matrix and recipe cookbook.
  - Run full test suite across all packages (`go test -v -count=1 ./...`).
  - Commit and push to `origin/main`.
