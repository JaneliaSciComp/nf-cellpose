process COLLECT_INPUTS {
    label 'process_single'
    container { task.ext.container }

    input:
    tuple path(input_path), val(input_pattern)

    output:
    eval('cat inputs.txt')

    when:
    task.ext.when == null || task.ext.when

    script:
    def inputs_fn = 'inputs.txt'
    def select_inputs_block
    if (input_pattern) {
        select_inputs_block = """
        if [ -d \${full_input_path} ]; then
            for f in \$(find \${full_input_path} -name "${input_pattern}" -maxdepth 1); do
                fn=\$(basename \${f})
                full_filepath="\${full_input_path}/\${fn}"
                echo "\${full_filepath}" >> ${inputs_fn}
            done
        else
            echo "Error ${input_path} (\${full_input_path}) must be a directory" >&2
            exit 1
        fi
        """
    } else {
        select_inputs_block = "echo \${full_input_path} > ${inputs_fn}"
    }
    """
    full_input_path=\$(readlink ${input_path})
    ${select_inputs_block}
    cat ${inputs_fn}
    """
}
