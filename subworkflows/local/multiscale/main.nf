include { MULTISCALE_PYRAMID } from '../../../modules/local/multiscale/pyramid/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MULTISCALE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow MULTISCALE {
    take:
    ch_meta       // ch: [ meta, img_container, img_dataset ]
    ch_dask_info  // ch: [ dask_scheduler, dask_config ]
    skip_flag     // boolean
    cpus          // number
    mem_gb        // number

    main:
    def multiscale_results
    def multiscale_version
    if (skip_flag) {
        multiscale_results = ch_meta
        multiscale_results.subscribe { log.debug "Skip multiscale pyramid: $it" }
        multiscale_version = Channel.empty()
    } else {
        MULTISCALE_PYRAMID(
            ch_meta,
            ch_dask_info,
            cpus,
            mem_gb,
        )
        multiscale_results = MULTISCALE_PYRAMID.out.data
        multiscale_results.subscribe { log.debug "Multiscale pyramid: $it" }
        multiscale_version = MULTISCALE_PYRAMID.out.versions
    }

    emit:
    results = multiscale_results
    versions = multiscale_version
}
