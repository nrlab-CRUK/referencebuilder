include { assemblyPath } from '../functions/functions'

process bowtie1Index
{
    label 'builder'

    publishDir "${assemblyPath(genomeInfo)}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path(indexDir)

    shell:
        indexDir = "bowtie-${params.BOWTIE1_VERSION}"

        """
        mkdir "!{indexDir}"
        cd "!{indexDir}"

        !{params.BOWTIE1} \
            "../!{fastaFile}" \
            "!{genomeInfo.base}"
        """
}

workflow bowtie1WF
{
    take:
        fastaChannel

    main:

        def processingCondition =
        {
            genomeInfo, fastaFile ->
            def bowtieBase = "${assemblyPath(genomeInfo)}/bowtie-${params.BOWTIE1_VERSION}/${genomeInfo.base}"
            def requiredFiles = [ file("${bowtieBase}.1.ebwt"), file("${bowtieBase}.rev.1.ebwt") ]
            return requiredFiles.any { !it.exists() }
        }

        processingChoice = fastaChannel.branch
        {
            doIt: processingCondition(it)
            done: true
        }

        bowtie1Index(processingChoice.doIt)

        presentChannel = processingChoice.done.map
        {
            genomeInfo, fastaFile ->
            tuple genomeInfo, file("${assemblyPath(genomeInfo)}/bowtie-${params.BOWTIE1_VERSION}")
        }

        bowtie1Channel = presentChannel.mix(bowtie1Index.out)

    emit:
        bowtie1Channel
}
