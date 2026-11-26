# Persona: Mathematician / DSP & Signal Processing Engineer

## Profile
- **Role**: Mathematician / DSP (Digital Signal Processing) Engineer / RF & Acoustic Modeling Engineer
- **Focus**: Signal transformations, frequency analysis, phase angle extraction, complex vector arithmetic ($z = x + iy$), filter design, and numerical pipelines.
- **Primary Tools**: Go, MATLAB / Python (NumPy/SciPy), FFTW, SDR (Software Defined Radio), Audio/RF capture hardware.

## Daily Realities & Workflows
- Streaming raw complex IQ samples from radio receivers or acoustic sensors.
- Transforming complex numbers into polar form (magnitude $\sqrt{x^2 + y^2}$, phase $\theta = \arctan(y/x)$).
- Computing exact vector summations, dot products, and signal energy integration over bounded chunks or continuous feeds.
- Implementing iterative algorithms (such as recursive FIR/IIR filtering or running phase accumulation) without memory allocations.

## Pain Points with Existing Solutions
- **Error Boilerplate Cluttering Math**: Standard streaming libraries force all functions to return `(T, error)` tuples, turning pure mathematical formulas $y = f(x)$ into noisy error-handling trees.
- **Missing Complex Number Primitives**: Most Go streaming and math libraries only support integers or floats, completely omitting `complex64` and `complex128`.
- **Performance Overhead**: In high-frequency signal pipelines, extra allocations or heap escapes per sample kill performance.

## What Makes Them Say: *"Damn, I can see how to use this"*
- `operators/pure`: Zero-boilerplate pure functional mappings (`pure.Map`, `pure.Filter`, `pure.Reduce`, `pure.Scan`) accepting `func(I) O` directly.
- `operators/math`: Native support for `constraints.Complex` in `Number` arithmetic (`math.Sum`, `math.RunningSum`).
- Ability to chain pure mathematical functions cleanly with standard Go `math` and `math/cmplx` packages.
