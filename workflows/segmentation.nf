/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { paramsSummaryMap                 } from 'plugin/nf-schema'
include { softwareVersionsToYAML           } from '../subworkflows/nf-core/utils_nfcore_pipeline'

include { COLLECT_INPUTS                   } from '../modules/local/collect_inputs'
include { SEGTOOLS_DISTRIBUTED_CELLPOSE    } from '../modules/janelia/segtools/distributed/cellpose/main'
include { SEGTOOLS_DISTRIBUTED_MERGELABELS } from '../modules/janelia/segtools/distributed/mergelabels/main'

include { DASK_START                       } from '../subworkflows/janelia/dask_start'
include { DASK_STOP                        } from '../subworkflows/janelia/dask_stop'
include { MULTISCALE                       } from '../subworkflows/janelia/multiscale'

workflow SEGMENTATION {

    main:

    def ch_versions = channel.empty()

    def meta = [ id: "segmentation" ]
    def input = file(params.input)
    def outputdir = file(params.outdir)
    def model_dir
    def model_name

    if (params.cellpose_model) {
        if (params.cellpose_model.startsWith('/')) {
            // model is set using an absolute path
            def full_model_path = file(params.cellpose_model)
            model_dir = full_model_path.parent
            model_name = full_model_path.name
            log.info("Use model ${model_name} located at: ${model_dir} (${params.cellpose_model})")
        } else {
            model_dir = params.cellpose_models_dir ? file(params.cellpose_models_dir) : file("${params.workdir}/cellpose_models")
            model_name = params.cellpose_model
            log.info("Use model ${model_name} (${params.cellpose_model}) that will be downloaded to ${model_dir}")
        }
    } else {
        model_dir = params.cellpose_models_dir ? file(params.cellpose_models_dir) : file("${params.workdir}/cellpose_models")
        model_name = ''
        log.info("Use default model that will be downloaded to ${model_dir}")
    }

    def dask_data = [
        input.parent,
        outputdir.parent,
        file(params.workdir),
        model_dir,
    ]

    def ch_dask_cluster = DASK_START(
        channel.of([meta, dask_data]),
        params.with_dask,
        params.dask_config,
        file("${params.workdir}/dask-work/${workflow.sessionId}"),
        params.dask_workers,
        params.dask_min_workers,
        params.dask_worker_cpus,
        (params.dask_worker_mem_gb as int) > 0
            ? (params.dask_worker_mem_gb as int)
            : (params.default_mem_gb_per_cpu as int) * (params.dask_worker_cpus as int),
    )

    def ch_data_inputs = channel.of([file(params.input), params.input_pattern])
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
            // create the name of the labels container by appending the labels_container_suffix param to the input container name
            def fsuffix = params.labels_container_suffix ? "${params.labels_container_suffix}" : ''
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

    def ch_cellpose_inputs = ch_dask_cluster
    | combine(ch_data_inputs, by:0)
    | multiMap { row ->
        def (cellpose_meta,
             dask_info,
             input_container,
             input_subpath,
             cellpose_output_dir,
             labels_container,
             labels_subpath) = row

        def cellpose_data = [
            cellpose_meta,
            input_container,
            input_subpath,
            params.input_mask ? file(params.input_mask) : [],
            params.input_mask_subpath,
            model_dir,
            model_name,
            cellpose_output_dir,
            labels_container,
            labels_subpath,
            file("${params.workdir}/cellpose-work/${workflow.sessionId}"),
        ]

        def cellpose_cluster = [
            dask_info.scheduler_address,
            params.dask_config ? file(params.dask_config) : [],
        ]

        log.info "Cellpose inputs: $row -> (${cellpose_data}, ${cellpose_cluster})"

        cellpose_data:    cellpose_data
        cellpose_cluster: cellpose_cluster
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Cellpose model evaluation (or reuse a prior run's output)
    // ch_labels shape: [ meta, image, image_subpath, labels_containers(\n-joined), labels_subpath ]
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def ch_labels
    if (params.run_cellposemasks) {
        def cellpose_outputs = SEGTOOLS_DISTRIBUTED_CELLPOSE(
            ch_cellpose_inputs.cellpose_data,
            ch_cellpose_inputs.cellpose_cluster,
            params.preprocessing_config ? file(params.preprocessing_config) : [],
            params.cellpose_log_config ? file(params.cellpose_log_config) : [],
        )
        ch_versions = ch_versions.concat(cellpose_outputs.versions)
        ch_labels = cellpose_outputs.results
        ch_labels.subscribe { result -> log.debug "Cellpose results: $result" }
    } else {
        // reuse an existing segmentation output (requires a prior successful eval)
        ch_labels = ch_cellpose_inputs.cellpose_data
        | map { cp_row ->
            def (sk_meta, image, image_subpath,
                 _mask, _mask_subpath,
                 _models_dir, _model_name,
                 output_dir, labels_container, labels_subpath,
                 _working_dir) = cp_row
            def existing_labels = "${output_dir}/${labels_container}"
            log.debug "run_cellposemasks=false: reusing existing labels ${existing_labels}"
            [ sk_meta, image, image_subpath, existing_labels, labels_subpath ?: image_subpath ]
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Optionally merge labels across blocks (distributed).
    // ch_segmentation_results shape (per labels container): [ meta, labels_container, labels_subpath ]
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def ch_segmentation_results
    if (params.run_mergelabels) {
        def mergelabels_inputs = ch_labels
        | combine(ch_dask_cluster, by: 0)
        | flatMap { row ->
            def (ml_meta,
                 _image, _image_subpath,
                 labels_containers, labels_subpath,
                 dask_info) = row
            labels_containers.split('\n')
            .findAll { lc -> lc }
            .collect { labels_container ->
                def ml_data = [
                    ml_meta,
                    file(labels_container),
                    labels_subpath,
                    [],   // mask
                    '',   // mask_subpath
                    [],   // output_labels => in-place (overwrite input container)
                    labels_subpath,   // output_subpath
                    file("${params.workdir}/cellpose-work/${workflow.sessionId}"),
                ]
                def ml_cluster = [
                    dask_info ? dask_info.scheduler_address : '',
                    params.dask_config ? file(params.dask_config) : [],
                ]
                log.info "Merge labels input: ${ml_data} on ${ml_cluster}"
                [ ml_data, ml_cluster ]
            }
        }

        def ch_mergelabels_outputs = SEGTOOLS_DISTRIBUTED_MERGELABELS(
            mergelabels_inputs.map { pair -> pair[0] },
            mergelabels_inputs.map { pair -> pair[1] },
            params.cellpose_log_config ? file(params.cellpose_log_config) : [],
        )
        ch_versions = ch_versions.concat(ch_mergelabels_outputs.versions)

        // ml.results: [ meta, input_labels_fullpath, input_subpath, output_labels_fullpath, output_subpath ]
        ch_segmentation_results = ch_mergelabels_outputs.results
        | map { ml_row ->
            def (sr_meta, _in_labels, _in_subpath, out_labels, out_subpath) = ml_row
            [ sr_meta, out_labels, out_subpath ]
        }
    } else {
        ch_segmentation_results = ch_labels
        | flatMap { row ->
            def (sr_meta,
                 _image, _image_subpath,
                 labels_containers, labels_subpath) = row
            labels_containers.split('\n')
            .findAll { lc -> lc }
            .collect { labels_container ->
                [ sr_meta, labels_container, labels_subpath ]
            }
        }
    }

    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Multiscale pyramid for the segmentation results (per labels container),
    // attaching the dask cluster info per item.
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def ch_multiscale_inputs = ch_segmentation_results
    | combine(ch_dask_cluster, by: 0)
    | map { row ->
        def (ms_meta, labels_container, labels_subpath, dask_info) = row
        def ms_data = [ ms_meta, labels_container, labels_subpath ]
        def ms_cluster = [
            dask_info ? dask_info.scheduler_address : '',
            params.dask_config ? file(params.dask_config) : [],
        ]
        log.info "Multiscale inputs: ${ms_data} on ${ms_cluster}"
        [ ms_data, ms_cluster ]
    }

    def ch_multiscale_outputs = MULTISCALE(
        ch_multiscale_inputs.map { pair -> pair[0] },
        ch_multiscale_inputs.map { pair -> pair[1] },
        params.run_multiscale,
    )

    // wait until all multiscale finish - this is still needed in case multiple inputs are being segmented
    def ch_all_multiscale_results = ch_multiscale_outputs
    | groupTuple(by:0)

    // stop the dask cluster
    ch_dask_cluster
    | join(ch_all_multiscale_results, by:0)
    | map { row ->
        def (cluster_meta, cluster_context) = row
        log.debug "Prepare to stop the dask cluster -> $row"
        [ cluster_meta, cluster_context ]
    }
    | DASK_STOP

    emit:
    results  = ch_multiscale_outputs
    versions = ch_versions     // channel: [ path(versions.yml) ]
}
