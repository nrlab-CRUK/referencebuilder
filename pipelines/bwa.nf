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

        !{params.BWA} index \
            -a bwtsw \
            -p "!{genomeInfo.base}" \
            "../!{fastaFile}"
        """
}

workflow bwaWF
{
    take:
        fastqChannel

    main:

        def processingCondition =
        {
            genomeInfo, fastaFile ->
            def bwaBase = "${assemblyPath(genomeInfo)}/bwa-${params.BWA_VERSION}/${genomeInfo.base}"
            def requiredFiles = [ file("${bwaBase}.bwt"), file("${bwaBase}.pac"), file("${bwaBase}.sa") ]
            return requiredFiles.any { !it.exists() }
        }

        processingChoice = fastqChannel.branch
        {
            doIt: processingCondition(it)
            done: true
        }

        bwaIndex(processingChoice.doIt)

        presentChannel = processingChoice.done.map
        {
            genomeInfo, fastaFile ->
            tuple genomeInfo, file("${assemblyPath(genomeInfo)}/bwa-${params.BWA_VERSION}")
        }

        bwaChannel = presentChannel.mix(bwaIndex.out)

    emit:
        bwaChannel
}
