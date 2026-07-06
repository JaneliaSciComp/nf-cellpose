# JaneliaSciComp/nf-cellpose pipeline flow

The `SEGMENTATION` workflow (see `workflows/segmentation.nf`) runs Cellpose
segmentation, optionally on a distributed Dask cluster, with an optional label
merge step.

```mermaid
flowchart TD
    start --> dask
    start -. "skip dask cluster" .-> eval
    dask["Start Dask cluster"] --> eval["Eval model"]
    eval --> merge["Merge labels"]
    eval -. "skip merge" .-> done([Segmentation results])
    merge --> done
```

- **Start Dask cluster** — optional (`params.with_dask`); when disabled, work runs without a distributed cluster.
- **Eval Cellpose model** — runs the blockwise Cellpose model evaluation to produce labels masks.
- **Merge labels** — merges labels across blocks; the `skip merge` arrow bypasses it (`params.run_mergelabels = false`).
