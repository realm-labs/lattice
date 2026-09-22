# Repository Instructions

## Rust module layout

- Use `foo.rs` as the root of module `foo` and place its submodules under `foo/`.
- Do not create `foo/mod.rs`; the repository uses one module layout consistently.
- When splitting an existing module, move its root declarations to `foo.rs` and keep child module files in `foo/`.
- Run `bash scripts/check-structure.sh` after structural changes. The check rejects `mod.rs` files.

## File size and maintainability

- Keep every Rust source file at or below 1,200 effective lines of code. Blank lines, comments, and documentation comments do not count. This is a hard limit enforced by `scripts/check-structure.sh`, not a target size.
- Never shorten, remove, or avoid useful documentation to reduce LOC. When implementation code approaches the limit, improve the module boundaries instead.
- Split a file before it reaches the limit when it already contains multiple responsibilities or distinct domains.
- Do not evade the line limit by compressing code, combining unrelated declarations, or moving a large implementation into one oversized nested module.
- Keep module root files focused on the public surface, shared types, and high-level orchestration. Put cohesive implementations in clearly named child modules.
- Give each module one clear responsibility. Prefer domain or capability names over vague buckets such as `common`, `misc`, or `utils`.
- Keep public APIs deliberate and minimal. Do not flatten module boundaries with broad re-exports unless the re-export is part of the intended public API.
- Keep dependencies directional: lower-level domain modules must not depend on higher-level orchestration modules. Extract a focused shared abstraction when two modules would otherwise depend on each other.
- Keep tests close to the behavior they verify, and split large test suites by behavior or subsystem rather than accumulating unrelated cases in one file.
- Optimize for readability and change isolation: a routine feature change should touch the smallest coherent set of modules possible.
