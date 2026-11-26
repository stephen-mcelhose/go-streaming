# Global Agent Instructions

## Workflow

- Keep commit messages formatted per Conventional Commits (`feat:`, `fix:`, `test:`, `refactor:`, `docs:`).
- Always ensure all tests pass before making a feature commit.

## Type-First Design & Implementation Protocol

When designing or implementing Go features:

1. **Top-Down Sketch (Domain Core):**
   - **MUST** write the orchestrator/entry point first as declarative domain prose.
   - **MUST** use domain types in core logic (`Receiver`, `Sender`, `Stream`).
   - **DO NOT** force domain terms into Ports & Adapters. Adapters translate wire/persistence types to domain types.

2. **Push Mechanisms to Leaf Nodes:**
   - **MUST NOT** put SQL, regex, serialization, math, or network calls in top-level orchestrators.
   - **MUST** delegate technical mechanics downward into leaf functions or consumer-side interfaces.

3. **Stub-First Compilation Gate:**
   - **MUST** stub all sub-functions. Stubs **MAY** return zero values or sentinel errors.
   - **MUST** verify `go build ./...` passes before writing any business logic or unit tests.

4. **Bottom-Up TDD:**
   - **MUST** write table-driven tests (`type testCase struct`) for leaf nodes first.
   - **MUST** implement and pass leaf tests before implementing caller orchestration.
   - **MUST** verify top-level integration test passes as Definition of Done.

## Go Table-Driven Test Layout (`testCase struct`)

When writing or maintaining Go tests:
- **Geometry:** Define `type testCase struct` **inside** the test function. Name the slice `testCases`, loop with `for _, tc := range testCases`, and execute with `t.Run(tc.name, func(t *testing.T) { ... })`.
- **Field Layout:** Follow the structural geometry:
  ```go
  type testCase struct {
  	name string

  	// setup (optional — construction / dependency injection / mocks)
  	setupMock func(...)

  	// inputs — names match the function signature
  	ctx    context.Context
  	stream Receiver[T]

  	// expected outputs — names match returns (prefer wantX / wantErr)
  	want    Result
  	wantErr error

  	// side effects (optional)
  	assertSideEffects func(t *testing.T, got Result)
  }
  ```
- **Inputs & Outputs:** Field names must match the signature of the function under test (avoid generic `in`/`out`).
- **Assertions:** Prefer `github.com/stretchr/testify/require` (fail-fast). Use `assert` only for independent multi-field checks. Return immediately after `require.Error(t, err)`.
- **Naming (RFC 2119):** Contract/specable behaviors: use uppercase RFC 2119 in `testCase.name` (e.g. `"MUST error when context is canceled"`).
