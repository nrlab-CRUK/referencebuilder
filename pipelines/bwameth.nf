include { assemblyPath } from '../functions/functions'

process bwamethIndex
{
    label 'builder'

    publishDir "${assemblyPath(genomeInfo)}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path(indexDir)

    shell:
        indexDir = "bwameth-${params.BWAMETH_VERSION}"

        """
        mkdir "!{indexDir}"
        cd "!{indexDir}"

        bwameth.py index-mem2 \
            "../!{fastaFile}"
        """
}

workflow bwamethWF
{
    take:
        fastaChannel

    main:
        processingChannel = fastaChannel
            .filter
            {
                genomeInfo, fastaFile ->
                def bwamethBase = "${assemblyPath(genomeInfo)}/bwameth-${params.BWAMETH_VERSION}/${genomeInfo.base}"
                def requiredFiles = [ file("${bwamethBase}.0123"), file("${bwamethBase}.bwt.2bit.64"), file("${bwamethBase}.pac") ]
                return requiredFiles.any { !it.exists() }
            }

        bwamethIndex(processingChannel)
}
