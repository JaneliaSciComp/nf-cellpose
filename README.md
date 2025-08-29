# JaneliaSciComp/nf-cellpose

[![GitHub Actions CI Status](https://github.com/JaneliaSciComp/nf-cellpose/actions/workflows/nf-test.yml/badge.svg)](https://github.com/JaneliaSciComp/nf-cellpose/actions/workflows/nf-test.yml)
[![GitHub Actions Linting Status](https://github.com/JaneliaSciComp/nf-cellpose/actions/workflows/linting.yml/badge.svg)](https://github.com/JaneliaSciComp/nf-cellpose/actions/workflows/linting.yml)[![Cite with Zenodo](http://img.shields.io/badge/DOI-10.5281/zenodo.XXXXXXX-1073c8?labelColor=000000)](https://doi.org/10.5281/zenodo.XXXXXXX)
[![nf-test](https://img.shields.io/badge/unit_tests-nf--test-337ab7.svg)](https://www.nf-test.com)

[![Nextflow](https://img.shields.io/badge/version-%E2%89%A524.10.5-green?style=flat&logo=nextflow&logoColor=white&color=%230DC09D&link=https%3A%2F%2Fnextflow.io)](https://www.nextflow.io/)
[![nf-core template version](https://img.shields.io/badge/nf--core_template-3.3.2-green?style=flat&logo=nfcore&logoColor=white&color=%2324B064&link=https%3A%2F%2Fnf-co.re)](https://github.com/nf-core/tools/releases/tag/3.3.2)
[![run with conda](http://img.shields.io/badge/run%20with-conda-3EB049?labelColor=000000&logo=anaconda)](https://docs.conda.io/en/latest/)
[![run with docker](https://img.shields.io/badge/run%20with-docker-0db7ed?labelColor=000000&logo=docker)](https://www.docker.com/)
[![run with singularity](https://img.shields.io/badge/run%20with-singularity-1d355c.svg?labelColor=000000)](https://sylabs.io/docs/)
[![Launch on Seqera Platform](https://img.shields.io/badge/Launch%20%F0%9F%9A%80-Seqera%20Platform-%234256e7)](https://cloud.seqera.io/launch?pipeline=https://github.com/JaneliaSciComp/nf-cellpose)

## Introduction

**JaneliaSciComp/nf-cellpose** is a pipeline that runs Cellpose segmentation on a Dask cluster.

The pipeline generates the image segmentation using [Cellpose](https://github.com/MouseLand/cellpose). Large images can be partitioned into smaller blocks and processed on a distributed Dask cluster. The input can be a single (Tiff) image file or single (OME-ZARR) image container, or a directory containing multiple input files or containers that need to be segmented. In the latter case when multiple inputs need to be segmented, the user must also provide an input pattern, used for selecting the images. The pipeline output is the Cellpose result saved as a Tiff or an OME-ZARR (depending on the input) at the location specified by the `outdir` parameter. For OME-ZARR, there's also an option to generate the multiscale pyramid for the labels image.



## Usage

> [!NOTE]
> If you are new to Nextflow and nf-core, please refer to [this page](https://nf-co.re/docs/usage/installation) on how to set-up Nextflow. Make sure to [test your setup](https://nf-co.re/docs/usage/introduction#how-to-run-a-pipeline) with `-profile test` before running the workflow on actual data.

Now, you can run the pipeline using:

```bash
nextflow run JaneliaSciComp/nf-cellpose \
   -profile <docker/singularity/.../institute> \
   --input input.ome.zarr \
   --outdir <OUTDIR>
```

> [!WARNING]
> Please provide pipeline parameters via the CLI or Nextflow `-params-file` option; an example of such file is available [here](examples/segtest_sample.json). Custom config files including those provided by the `-c` Nextflow option can be used to provide any configuration _**except for parameters**_; see [docs](https://nf-co.re/docs/usage/getting_started/configuration#custom-configuration-files).

## Credits

[Cellpose-SAM](https://github.com/MouseLand/cellpose) used in this pipeline was developed by Marius Pachitariu, Michael Rariden and Carsen Stringer

The distributed Cellpose was the contributed to [cellpose](https://github.com/MouseLand/cellpose/pull/408) by Greg Fleishman

JaneliaSciComp/nf-cellpose was originally written by Cristian Goina.

We thank the following people for their extensive assistance in the development of this pipeline:


## Contributions and Support

If you would like to contribute to this pipeline, please see the [contributing guidelines](.github/CONTRIBUTING.md).

## Citations

An extensive list of references for the tools used by the pipeline can be found in the [`CITATIONS.md`](CITATIONS.md) file.

This pipeline uses code and infrastructure developed and maintained by the [nf-core](https://nf-co.re) community, reused here under the [MIT license](https://github.com/nf-core/tools/blob/main/LICENSE).

> **The nf-core framework for community-curated bioinformatics pipelines.**
>
> Philip Ewels, Alexander Peltzer, Sven Fillinger, Harshil Patel, Johannes Alneberg, Andreas Wilm, Maxime Ulysse Garcia, Paolo Di Tommaso & Sven Nahnsen.
>
> _Nat Biotechnol._ 2020 Feb 13. doi: [10.1038/s41587-020-0439-x](https://dx.doi.org/10.1038/s41587-020-0439-x).
