#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { fastaWF } from './pipelines/fasta'

workflow
{
    genomeInfoChannel = channel
        .fromPath("${projectDir}/genomeinfo/full/*.properties")
        .map
        {
            propsFile ->
            def props = new Properties()
            propsFile.withReader { props.load(it) }
            tuple "${props['name.scientific']} ${props['version']}", props
        }
    
    fastaWF(genomeInfoChannel)
}
