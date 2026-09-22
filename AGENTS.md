# Repository Instructions

## Rust module layout

- Use `foo.rs` as the root of module `foo` and place its submodules under `foo/`.
- Do not create `foo/mod.rs`; the repository uses one module layout consistently.
- When splitting an existing module, move its root declarations to `foo.rs` and keep child module files in `foo/`.
- Run `bash scripts/check-structure.sh` after structural changes. The check rejects `mod.rs` files.
