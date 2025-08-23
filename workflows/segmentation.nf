/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { paramsSummaryMap       } from 'plugin/nf-schema'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'

include { COLLECT_INPUTS         } from '../modules/local/collect_inputs'
include { CELLPOSE               } from '../modules/janelia/cellpose/main'

include { DASK_START             } from '../subworkflows/janelia/dask_start/main'
include { DASK_STOP              } from '../subworkflows/janelia/dask_stop/main'
include { MULTISCALE             } from '../subworkflows/local/multiscale/main'

workflow SEGMENTATION {

    main:

    def ch_versions = Channel.empty()

    def session_work_dir = "${params.workdir}/${workflow.sessionId}"
    def meta = [ id: "segmentation" ]
    def input = file(params.input)
    def outputdir = file(params.outdir)
    def model_dir
    def model_name

    if (params.cellpose_model) {
        if (params.cellpose_model.startsWith('/')) {
            // model is set using an absolute path
            def full_model_path = file(model_name)
            model_dir = full_model_path.parent
            model_name = full_model_path.name
        } else {
            model_dir = params.cellpose_models_dir ? file(params.cellpose_models_dir) : file("${session_work_dir}/cellpose_models")
            model_name = params.cellpose_model
        }
    } else {
        model_dir = params.cellpose_mnodels_dir ? file(params.cellpose_models_dir) : file("${session_work_dir}/cellpose_models")
        model_name = ''
    }

    def dask_data = [
        input.parent,
        outputdir.parent,
        file(session_work_dir),
        model_dir,
    ]

    def dask_cluster = DASK_START(
        Channel.of([meta, dask_data]),
        params.with_dask,
        params.dask_config,
        session_work_dir,
        params.dask_workers,
        params.dask_min_workers,
        params.dask_worker_cpus,
        params.dask_worker_mem_gb ?: params.default_mem_gb_per_cpu * params.dask_worker_cpus,
    )

    def ch_data_inputs = Channel.of([file(params.input), params.input_pattern])
        | COLLECT_INPUTS
        | splitText
        | map { input_name ->
            def finput = input_name.trim()
            def fn_with_ext = file(finput).name
            def fn
            def fext
            def ext_idx = fn_with_ext.lastIndexOf('.')
            if (ext_idx == -1) {
                fn = fn_with_ext
                fext = ''
            } else {
                fn = fn_with_ext.substring(0, ext_idx)
                fext = fn_with_ext.substring(ext_idx)
            }
            if (params.labels_container_ext) {
                fext = params.labels_container_ext
            }
            if (!fext || fext == '.') {
                error "The input filename - '${finput}' has no extension and no labels_container_ext is set. File extension is required for labels container to know how to save the output. Use --labels_container_ext to set it"
            }
            def fsuffix = params.labels_container_suffix ? "-${params.labels_container_suffix}" : ''

            def labels_container = "${fn}${fsuffix}${fext}" 
            def labels_subpath
            if (params.labels_group && params.input_subpath ) {
                labels_subpath = "${params.labels_group}/${params.input_subpath}"
            } else if (params.labels_group) {
                labels_subpath = params.labels_group
            } else if (params.input_subpath) {
                labels_subpath = params.input_subpath
            } else {
                labels_subpath = ''
            }

            def r = [
                meta, 
                finput,
                params.input_subpath,
                outputdir,
                labels_container,
                labels_subpath,
            ]
            log.info "Segmentation input: $r"
            r
        }

    def cellpose_inputs = dask_cluster
    | combine(ch_data_inputs, by:0)
    | multiMap {
        def (cellpose_meta,
             dask_info,
             input_container,
             input_subpath,
             cellpose_output_dir,
             labels_container,
             labels_subpath) = it

        def cellpose_data = [
            cellpose_meta,
            input_container,
            input_subpath,
            model_dir,
            model_name,
            cellpose_output_dir,
            labels_container,
            labels_subpath,
            file("${session_work_dir}/cellpose-work"),
        ]

        def cellpose_cluster = [
            dask_info.scheduler_address,
            params.dask_config ? file(params.dask_config) : [],
        ]

        log.info "Cellpose inputs: $it -> (${cellpose_data}, ${cellpose_cluster})"

        cellpose_data:    cellpose_data
        cellpose_cluster: cellpose_cluster
    }

    def cellpose_outputs = CELLPOSE(
        cellpose_inputs.cellpose_data,
        cellpose_inputs.cellpose_cluster,
        params.preprocessing_config ? file(params.preprocessing_config) : [],
        params.cellpose_log_config ? file(params.cellpose_log_config) : [],
        params.cellpose_cpus,
        params.cellpose_mem_gb ?: params.default_mem_gb_per_cpu * params.cellpose_cpus,
    )

    def cellpose_results = cellpose_outputs.results
    cellpose_results.subscribe {
        log.debug "Cellpose results: $it"
    }

    // append cellpose version
    ch_versions = ch_versions.concat (cellpose_outputs.versions)

    def multiscale_inputs = cellpose_results
    | flatMap {
        def (cellpose_meta,
             input_container, input_subpath,
             labels_containers, labels_subpath) = it
        labels_containers.split('\n')
        .findAll { it }
        .collect { labels_container ->
            def r = [
                cellpose_meta, labels_container, labels_subpath,
            ]
            log.info "Multiscale inputs: $r"
            r
        }
    }

    def multiscale_outputs = MULTISCALE(
        multiscale_inputs,
        cellpose_inputs.cellpose_cluster,
        params.skip_multiscale,
        params.multiscale_cpus,
        params.multiscale_mem_gb ?: params.default_mem_gb_per_cpu * params.multiscale_cpus,
    )

    // append multiscale version
    ch_versions = ch_versions.concat (multiscale_outputs.versions)

    // stop the dask cluster
    dask_cluster
    | join(multiscale_outputs.results, by:0)
    | map {
        def (cluster_meta, cluster_context) = it
        [ cluster_meta, cluster_context ]
    }
    | DASK_STOP

    emit:
    results  = multiscale_outputs.results
    versions = ch_versions     // channel: [ path(versions.yml) ]
}
