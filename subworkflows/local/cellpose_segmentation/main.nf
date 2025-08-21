include { CELLPOSE   } from '../../modules/janelia/cellpose/main.nf'

include { DASK_START } from '../janelia/dask_start/main.nf'
include { DASK_STOP  } from '../janelia/dask_stop/main.nf'

workflow CELLPOSE_SEGMENTATION {
    take:
    ch_cellpose_data      // channel: [ meta,
                          //            input_image_container, input_dataset, 
                          //            output_dir,
                          //            labels_image_container, labels_dataset ]
    dask_scheduler        // dask_scheduler
    dask_config           // dask_config
    models_dir            // string|file - location of the cellpose models
    model_name            // string - segmentation model name
    segmentation_work_dir // string|file: segmentation work dir
    log_config            // string|file - log configuration
    segmentation_cpus     // int: number of cores to use for segmentation main process
    segmentation_mem_gb    // int: number of GB of memory to use for segmentation main process

    main:
    def cellpose_inputs = ch_cellpose_data
    | map {
        def (meta, input_image_container, input_dataset, output_dir, labels_image_container, labels_dataset) = it

        [
            meta,
            input_image_container,
            input_dataset,
            models_dir ? file(models_dir) : [],
            model_name,
            output_dir,
            labels_image_container,
            labels_dataset
        ]
    }

    def cellpose_outputs = CELLPOSE(
        cellpose_inputs,
        ch_dask,
        log_config ? file(log_config): [],
        segmentation_cpus,
        segmentation_mem_gb,
    )

    emit:
    results = cellpose_outputs.results
    versions = cellpose_outputs.version
}