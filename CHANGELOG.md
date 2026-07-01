# JaneliaSciComp/nf-cellpose: Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v1.1.0 - 2026-07-01

### `Added`

- Nextflow V2 parser compliance
- Zarr v3 support
- Conda configuration - allows one to run the pipeline using a conda profile

## v1.0.0 - 2025-09-08

Initial release of JaneliaSciComp/nf-cellpose, created with the [nf-core](https://nf-co.re/) template.

### `Added`

- Dask support
- CellposeSAM support

### `Dependencies`
- Nextflow 25.04.04
- Modules:
    - JaneliaSciComp/nextflow-modules:segtools/distributed/cellpose
    - JaneliaSciComp/nextflow-modules:segtools/distributed/cellpose
    - JaneliaSciComp/nextflow-modules:omezarrtools/multiscale
- Sumworkflows:
    - JaneliaSciComp/nextflow-modules:dask_[start,stop]
