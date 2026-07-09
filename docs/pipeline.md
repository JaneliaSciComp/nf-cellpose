# JaneliaSciComp/nf-cellpose pipeline flow

The `SEGMENTATION` workflow (see `workflows/segmentation.nf`) runs Cellpose
segmentation, optionally on a distributed Dask cluster, with an optional label
merge step.

```mermaid
flowchart TD
    input[Collect Input] -- params.with_dask --> start_dask[Start Dask Cluster]
    start_dask --> eval[Cellpose Eval]
    input -.-> eval
    eval -- params.run_mergelabels --> merge[Merge Labels]
    merge --> multiscale[Multiscale]
    eval -.-> multiscale
```


- **Start Dask** — optional (`params.with_dask`); when disabled, work runs without a distributed cluster.
- **Eval Cellpose model** — runs the blockwise Cellpose model evaluation to produce labels masks.
- **Merge labels** — merges labels across blocks; the `skip merge` arrow bypasses it (`params.run_mergelabels = false`).
