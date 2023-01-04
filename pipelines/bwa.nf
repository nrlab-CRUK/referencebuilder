include { assemblyPath } from '../functions/functions'

process bwaIndex
{
    label 'builder'

    publishDir "${assemblyPath(genomeInfo)}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path(indexDir)

    shell:
        indexDir = "bwa-${params.BWA_VERSION}"

        """
        mkdir "!{indexDir}"
        cd "!{indexDir}"

        bwa index \
            -a bwtsw \
            -p "!{genomeInfo.base}" \
            "../!{fastaFile}"
        """
}

workflow bwaWF
{
    take:
        fastaChannel

    main:
        processingChannel = fastaChannel
            .filter
            {
                genomeInfo, fastaFile ->
                def bwaBase = "${assemblyPath(genomeInfo)}/bwa-${params.BWA_VERSION}/${genomeInfo.base}"
                def requiredFiles = [ file("${bwaBase}.bwt"), file("${bwaBase}.pac"), file("${bwaBase}.sa") ]
                return requiredFiles.any { !it.exists() }
            }

        bwaIndex(processingChannel)
}
