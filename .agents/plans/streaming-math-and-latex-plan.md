# Plan: Streaming Math Operators & `goexeclatex` Integration

## Overview
Expand `github.com/stephen-mcelhose/go-streaming` with advanced single-pass online mathematical and statistical primitives, and define the integration blueprint for declarative LaTeX formula evaluation using `github.com/stephen-mcelhose/goexeclatex`.

---

## 1. Mathematical Streaming Operators (`operators/math/`)

### A. Online Covariance & Pearson Correlation ($r_{XY}$)
- **Formulation**: Bivariate online recurrence extending Welford's algorithm to compute the sample co-moment $C_n = \sum (x_i - \bar{x}_n)(y_i - \bar{y}_n)$:
  $$C_n = C_{n-1} + (x_n - \bar{x}_{n-1})(y_n - \bar{y}_n)$$
  $$\text{Cov}(X, Y) = \frac{C_n}{n - 1}, \quad r_{XY} = \frac{C_n}{\sqrt{M_{2,X} M_{2,Y}}}$$
- **Complexity**: $\mathcal{O}(1)$ time per sample, $\mathcal{O}(1)$ memory (tracks $n, \bar{x}, \bar{y}, M_{2,X}, M_{2,Y}, C$).
- **Use Cases**: Live telemetry feature correlation (e.g. CPU load vs latency), paired asset spread tracking in quantitative finance.
- **Academic & Verifiable References**:
  - Donald Knuth, *The Art of Computer Programming, Vol. 2: Seminumerical Algorithms* (3rd ed., 1997), Section 4.2.2.
  - B. P. Welford, "Note on a method for calculating corrected sums of squares and products," *Technometrics*, 4(3):419–420, 1962. [DOI: 10.1080/00401706.1962.10490022](https://doi.org/10.1080/00401706.1962.10490022).
  - Wikipedia: [Algorithms for calculating variance - Covariance](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Covariance).

### B. Higher-Order Moments: Skewness & Kurtosis (Pébay's Moments)
- **Formulation**: Single-pass calculation of third central moment $M_3$ (skewness/asymmetry) and fourth central moment $M_4$ (kurtosis/tail fatness):
  $$\text{Skew} = \frac{\sqrt{n} M_3}{M_2^{3/2}}, \quad \text{Kurt} = \frac{n M_4}{M_2^2} - 3$$
- **Complexity**: $\mathcal{O}(1)$ time per sample, $\mathcal{O}(1)$ memory.
- **Use Cases**: Detecting asymmetric tail risk or latency distribution drift before threshold violations occur.
- **Academic & Verifiable References**:
  - Philippe Pébay, "Formulas for Robust, One-Pass Parallel Computation of Covariances and Arbitrary-Order Statistical Moments," Sandia National Laboratories Technical Report SAND2008-6212, 2008. [Sandia Report PDF](https://prod-ng.sandia.gov/techlib-noauth/access-control.cgi/2008/086212.pdf) / [ResearchGate](https://www.researchgate.net/publication/228723223_Formulas_for_Robust_One-Pass_Parallel_Computation_of_Covariances_and_Arbitrary-Order_Statistical_Moments).
  - Wikipedia: [Algorithms for calculating variance - Higher-order statistics](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics).

### C. Mean First Passage Time (MFPT)
- **Formulation**: Renewal hitting-time tracker $\tau_k = \inf \{ t > t_{k-1} : X(t) \ge B \}$, computing empirical expectation $\mathbb{E}[\tau]$ and variance $\text{Var}(\tau)$ via an underlying `Welford` accumulator upon barrier crossing.
- **Complexity**: $\mathcal{O}(1)$ time per sample, $\mathcal{O}(1)$ memory.
- **Use Cases**: Barrier option pricing, Leaky Integrate-and-Fire neuron firing rates, Mean Time Between Failures (MTBF).
- **Academic & Verifiable References**:
  - Sidney Redner, *A Guide to First-Passage Processes*, Cambridge University Press, 2001. [Cambridge Books Online](https://doi.org/10.1017/CBO9780511606014).
  - S. Condamin, O. Bénichou, V. Tejedor, R. Voituriez, J. Klafter, "First-passage times in complex scale-invariant media," *Nature*, 450:77–80, 2007. [Nature DOI: 10.1038/nature06201](https://doi.org/10.1038/nature06201).
  - Wikipedia: [First-hitting-time model](https://en.wikipedia.org/wiki/First-hitting-time_model).

### D. Streaming CUSUM (Cumulative Sum Control Chart)
- **Formulation**: Page's change-point detection algorithm:
  $$S_t^+ = \max(0, S_{t-1}^+ + X_t - (\mu_0 + K)), \quad S_t^- = \min(0, S_{t-1}^- + X_t - (\mu_0 - K))$$
  Trigger when $S_t^+ > H$ or $S_t^- < -H$.
- **Complexity**: $\mathcal{O}(1)$ time per sample, $\mathcal{O}(1)$ memory.
- **Use Cases**: Rapid detection of abrupt mean shifts and system degradations.
- **Academic & Verifiable References**:
  - E. S. Page, "Continuous Inspection Schemes," *Biometrika*, 41(1/2):100–115, 1954. [Biometrika Oxford Academic](https://doi.org/10.1093/biomet/41.1-2.100).
  - E. S. Page, "Cumulative Sum Charts," *Technometrics*, 3(1):1–9, 1961. [JSTOR](https://www.jstor.org/stable/1266470).
  - NIST/SEMATECH e-Handbook of Statistical Methods: [CUSUM Control Charts](https://www.itl.nist.gov/div898/handbook/pmc/section3/pmc323.htm).
  - Wikipedia: [CUSUM](https://en.wikipedia.org/wiki/CUSUM).

---

## 2. `goexeclatex` Integration (`operators/latex/` or `adapters/latex/`)

### A. Pre-compiled AST Engine in `goexeclatex`
- Introduce `goexeclatex.Compile(expr string) (*Program, error)` to eliminate runtime tokenization/parsing overhead on streaming hot paths.
- Support index-based variable slot bindings (`Program.EvalFast(vars []float64)`) for zero-allocation stream execution.

### B. Streaming LaTeX Operators
- `latex.Map`: Transforms variable streams (e.g. `map[string]float64` or `[]float64`) using compiled LaTeX expressions (`\sqrt{x^2 + y^2}`).
- `latex.Filter`: Evaluates mathematical inequality conditions written in LaTeX (`x^2 + y^2 \ge 100`).
- `latex.Scan`: Evaluates recurrence relations ($X_t = \alpha X_{t-1} + (1-\alpha)Y_t$).

---

## 3. Implementation Steps & Milestones

1. **Phase 1: Bivariate Welford & Correlation**
   - Create `operators/math/bivariate.go` and `bivariate_test.go` (`BivariateWelford`, `Covariance`, `RunningCovariance`, `PearsonCorrelation`).
2. **Phase 2: Skewness, Kurtosis & CUSUM**
   - Implement `operators/math/moments.go` and `operators/math/cusum.go` with RFC 2119 table-driven tests.
3. **Phase 3: Mean First Passage Time (MFPT)**
   - Implement `operators/math/mfpt.go` with renewal and multi-trial support.
4. **Phase 4: LaTeX Integration Spike**
   - Define compiled execution API in `goexeclatex` and demonstrate `latex.Map` over `go-streaming.Receiver`.
