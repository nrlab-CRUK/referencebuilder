include { assemblyPath } from '../functions/functions'

process starIndexWithGTF
{
    time = '12h'
    cpus = 8

    memory = { 45.GB * 2 ** (task.attempt - 1) } // So 45, 90, 180
    maxRetries = 2

    publishDir "${assemblyPath(genomeInfo)}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile), path(gtfFile)

    output:
        tuple val(genomeInfo), path(indexDir)

    shell:
        log.debug "STAR attempt ${task.attempt} on ${genomeInfo.base} uses ${task.memory.bytes} (${task.memory.giga} GB)."

        indexDir = "star-${params.STAR_VERSION}"
        indexLength = genomeInfo.getOrDefault('star.SAindexLength', 14)

        template 'star/starIndex.sh'
}

process starIndexNoGTF
{
    time = '12h'
    cpus = 8

    memory = { 45.GB * 2 ** (task.attempt - 1) } // So 45, 90, 180
    maxRetries = 2

    publishDir "${assemblyPath(genomeInfo)}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile), val(nothing)

    output:
        tuple val(genomeInfo), path(indexDir)

    shell:
        log.debug "STAR attempt ${task.attempt} on ${genomeInfo.base} uses ${task.memory.bytes} (${task.memory.giga} GB)."

        indexDir = "star-${params.STAR_VERSION}"
        indexLength = genomeInfo.getOrDefault('star.SAindexLength', 14)

        template 'star/starIndexNoGTF.sh'
}

workflow starWF
{
    take:
        fastaChannel
        gtfChannel

    main:
        // Combine the channels. Use the genome info 'base' as the key.
        info2Channel = fastaChannel.map { info, fasta -> tuple info.base, info }
        fasta2Channel = fastaChannel.map { info, fasta -> tuple info.base, fasta }
        gtf2Channel = gtfChannel.map { info, gtf -> tuple info.base, gtf }

        def gtfCondition =
        {
            genomeInfo, fastaFile, gtfFile ->
            return gtfFile != null
        }

        starInputChoice =
            info2Channel
            .join(fasta2Channel)
            .join(gtf2Channel, remainder: true)
            .map
            {
                base, genomeInfo, fastaFile, gtfFile ->
                tuple genomeInfo, fastaFile, gtfFile
            }
            .branch
            {
                GTF:   gtfCondition(it)
                noGTF: true
            }

        def processingCondition =
        {
            genomeInfo, fastaFile, gtfFile ->
            def starDir = "${assemblyPath(genomeInfo)}/star-${params.STAR_VERSION}"
            def requiredFiles = [ file("${starDir}/SA"), file("${starDir}/SAindex"), file("${starDir}/Genome") ]
            return requiredFiles.any { !it.exists() }
        }

        starIndexWithGTF(starInputChoice.GTF.filter { processingCondition(it) })

        starIndexNoGTF(starInputChoice.noGTF.filter { processingCondition(it) })
}
