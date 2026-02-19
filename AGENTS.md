## Purpose
- This file defines coding behavior for this TypeScript x Bun project.
- The goal is simple, readable, and fundamentally correct code.

## Core Principles
- Prefer less code that achieves more.
- Keep logic straightforward and close to where it is used.
- Avoid over-splitting functions and avoid unnecessary abstraction.
- Prefer Bun-native APIs first (`Bun.file`, `Bun.write`, `Bun.spawn`, `Bun.Glob`, `Bun.CryptoHasher`, `bun:sqlite`).
- Add third-party dependencies only when built-in options are insufficient.
- Fix root causes across dependencies, not only local symptoms.

## Naming and Structure
- Prefer short and clear names (one word when practical).
- If one-word names reduce clarity, use a longer explicit name.
- Avoid clever abbreviations.
- Split functions only when reuse is clear or readability clearly improves.

## Type Rules
- Prefer type inference from values and return types.
- Avoid `any` as much as possible.
- Use `unknown` for untrusted input, then narrow immediately.
- Introduce custom types only when they prevent real bugs, clarify boundaries, or are reused.
- Do not use `as` by default.
- Prefer control-flow narrowing, type guards, `in` checks, `instanceof`, `satisfies`, and `as const`.
- Allow `as` only at validated boundaries or when narrowing is not expressible.
- Avoid `as any`.
- Avoid chained assertions like `as unknown as T` unless there is no practical alternative.

## Error Handling
- Do not use exceptions for normal control flow.
- Prefer guard clauses and early returns for expected failures.
- Use `try/catch` only at boundaries (CLI entrypoint, worker loop, HTTP handler boundary).
- Never swallow errors; attach context and fail clearly once.

## Comments and Docs
- Avoid unnecessary comments and documentation.
- Do not write comments or docs that restate obvious code.
- Add comments only for non-obvious intent, constraints, or tradeoffs.
- Keep documentation minimal and operationally useful.

## Quality Bar
- Validate related paths after a fix, not only the changed line.
- Add or update regression tests for the actual failure path.
- Prefer durable fixes over local hotfixes.
- Brevity is good only when correctness and readability stay high.
