## Scope
- This file defines coding behavior for this TypeScript x Bun project.
- The goal is simple, readable, and fundamentally correct code.

## Core Rules
- Prefer less code that achieves more.
- Avoid `try/catch` as much as possible.
- Avoid `any` as much as possible.
- Avoid over-splitting functions or using overly complex techniques.
- Keep logic straightforward and co-located when it improves clarity.
- Prefer short, clear names (one word when practical).
- Avoid unnecessary comments and documentation.
- Prefer Bun-native APIs first.
- Prefer existing type inference and avoid new custom types unless necessary.
- Fix root causes across dependencies, not only local symptoms.

## Runtime and API Policy
- Use Bun-native APIs first: `Bun.file`, `Bun.write`, `Bun.spawn`, `Bun.Glob`, `Bun.CryptoHasher`, `bun:sqlite`.
- Add third-party dependencies only when built-in options are not sufficient.

## Error Handling Policy
- Do not use exceptions for normal control flow.
- Prefer guard clauses and early returns for expected failures.
- Use `try/catch` only at boundaries (CLI entrypoint, worker loop, HTTP handler boundary).
- Never swallow errors; attach context and fail clearly once.

## Naming Policy
- Use short, clear names.
- Prefer one-word names in local scope.
- If a one-word name hurts readability, use a longer explicit name.
- Avoid clever abbreviations.

## Type Policy
- Prefer inference from values and return types.
- Use `unknown` for untrusted external input, then narrow immediately.
- Introduce explicit custom types only when they prevent real bugs, clarify module boundaries, or are reused.

## Structure Policy
- Start with one direct implementation path.
- Split functions only when reuse is clear or readability clearly improves.
- Avoid abstractions created only for hypothetical future needs.

## Comment and Docs Policy
- Do not write comments or docs that only restate obvious code.
- Add comments only when they explain non-obvious intent, constraints, or tradeoffs.
- Keep documentation minimal and focused on operationally useful information.

## Root-Cause Policy
- Trace cause chains through callers, callees, data contracts, schema, and side effects.
- Validate related paths after the fix.
- Add or update regression tests for the real failure path.
- Prefer durable fixes over local hotfixes.

## Tradeoff Rules
- Shorter code is good only when readability and correctness stay high.
- Local simplicity is not enough if system behavior stays inconsistent.
- Use one-word naming as a default, not as a hard constraint.
