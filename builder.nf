#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

@Grab('org.apache.commons:commons-lang3:3.12.0')

import static org.apache.commons.lang3.StringUtils.isNotEmpty

include { setupWF } from './pipelines/setup'
include { fastaWF } from './pipelines/fasta'
include { annotationWF } from './pipelines/annotation'
include { bwaWF } from './pipelines/bwa'
include { bwamem2WF } from './pipelines/bwamem2'
include { bowtie1WF } from './pipelines/bowtie1'
include { starWF } from './pipelines/star'
include { salmonWF } from './pipelines/salmon'

def readGenomeInfo(propsFile)
{
    def genomeInfo = new Properties()
    propsFile.withReader { genomeInfo.load(it) }

    // Add some derived information for convenience.

    genomeInfo['species'] = genomeInfo['name.scientific'].toLowerCase().replace(' ', '_')
    genomeInfo['base'] = genomeInfo['abbreviation'] + '.' + genomeInfo['version']

    def transcriptUrl = genomeInfo['url.transcripts.fasta']
    genomeInfo['gencode'] = isNotEmpty(transcriptUrl) && transcriptUrl.startsWith("ftp://ftp.ebi.ac.uk/pub/databases/gencode");

    return genomeInfo
}

workflow
{
    setupWF()

    genomeInfoChannel = channel
        .fromPath("${projectDir}/genomeinfo/full/*.properties")
        .map { readGenomeInfo(it) }

    fastaWF(genomeInfoChannel)
    annotationWF(genomeInfoChannel, setupWF.out)
    bwaWF(fastaWF.out)
    bwamem2WF(fastaWF.out)
    bowtie1WF(fastaWF.out)
    starWF(fastaWF.out, annotationWF.out.gtfChannel)
    salmonWF(fastaWF.out)
}
