include { assemblyPath } from '../functions/functions'

process bwaIndex
{
    label 'bwa'

    publishDir "${assemblyPath(genomeInfo)}/bwa-${params.BWA_VERSION}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path('index')

    shell:
        """
        mkdir index
        cd index

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
            def bwaDir = "${assemblyPath(genomeInfo)}/bwa-${params.BWA_VERSION}"
            def requiredFiles = [
                file("${bwaDir}/${genomeInfo.base}.bwt"),
                file("${bwaDir}/${genomeInfo.base}.pac"),
                file("${bwaDir}/${genomeInfo.base}.sa")
            ]
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
