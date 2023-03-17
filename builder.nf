#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { readGenomeInfo } from './functions/functions'

include { setupWF } from './pipelines/setup'
include { genomeInfoWF } from './pipelines/info'
include { fastaWF } from './pipelines/fasta'
include { annotationWF } from './pipelines/annotation'
include { geneNamesWF } from './pipelines/geneNames'
include { bowtie2WF } from './pipelines/bowtie2'
include { bwamem2WF } from './pipelines/bwamem2'
include { bwamethWF } from './pipelines/bwameth'
include { dbsnpWF } from './pipelines/dbsnp'
include { cosmicWF } from './pipelines/cosmic'
include { blacklistWF } from './pipelines/blacklist'

workflow
{
    setupWF()

    genomeInfoFileChannel = channel
        .fromPath("${params.genomeInfoDirectory}/*.properties")

    genomeInfoWF(genomeInfoFileChannel)

    genomeInfoChannel = genomeInfoFileChannel
        .map { readGenomeInfo(it) }

    fastaWF(genomeInfoChannel)
    annotationWF(genomeInfoChannel, setupWF.out)
    geneNamesWF(genomeInfoChannel)
    bowtie2WF(fastaWF.out.fastaChannel)
    bwamem2WF(fastaWF.out.fastaChannel)
    bwamethWF(fastaWF.out.fastaChannel)
    dbsnpWF(genomeInfoChannel)
    cosmicWF(genomeInfoChannel)
    blacklistWF(genomeInfoChannel)
}
