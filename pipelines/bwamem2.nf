include { assemblyPath } from '../functions/functions'

process bwamem2Index
{
    label 'bwa'

    publishDir "${assemblyPath(genomeInfo)}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path(indexDir)

    shell:
        indexDir = "bwamem2-${params.BWAMEM2_VERSION}"

        """
        mkdir "!{indexDir}"
        cd "!{indexDir}"

        ${params.BWAMEM2} index \
            -p "!{genomeInfo.base}" \
            "../!{fastaFile}"
        """
}

workflow bwamem2WF
{
    take:
        fastqChannel

    main:

        def processingCondition =
        {
            genomeInfo, fastaFile ->
            def bwamemDir = "${assemblyPath(genomeInfo)}/bwamem2-${params.BWAMEM2_VERSION}"
            def requiredFiles = [
                file("${bwamemDir}/${genomeInfo.base}.0123"),
                file("${bwamemDir}/${genomeInfo.base}.bwt.2bit.64"),
                file("${bwamemDir}/${genomeInfo.base}.pac")
            ]
            return requiredFiles.any { !it.exists() }
        }

        processingChoice = fastqChannel.branch
        {
            doIt: processingCondition(it)
            done: true
        }

        bwamem2Index(processingChoice.doIt)

        presentChannel = processingChoice.done.map
        {
            genomeInfo, fastaFile ->
            tuple genomeInfo, file("${assemblyPath(genomeInfo)}/bwamem2-${params.BWAMEM2_VERSION}")
        }

        bwamem2Channel = presentChannel.mix(bwamem2Index.out)

    emit:
        bwamem2Channel
}
