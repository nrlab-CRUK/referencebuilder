#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { fastaWF } from './pipelines/fasta'

def readGenomeInfo(propsFile)
{
    def genomeInfo = new Properties()
    propsFile.withReader { genomeInfo.load(it) }

    // Add some derived information for convenience.

    genomeInfo['species'] = genomeInfo['name.scientific'].toLowerCase().replace(' ', '_')
    genomeInfo['base'] = genomeInfo['abbreviation'] + '.' + genomeInfo['version']

    return genomeInfo
}

workflow
{
    genomeInfoChannel = channel
        .fromPath("${projectDir}/genomeinfo/full/*.properties")
        .map { readGenomeInfo(it) }

    fastaWF(genomeInfoChannel)
}
