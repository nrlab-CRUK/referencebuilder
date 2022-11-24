include { assemblyPath } from '../functions/functions'

process starIndex
{
    time = '12h'
    cpus = 8

    memory = { 45.GB * 2 ** (task.attempt - 1) } // So 45, 90, 180
    maxRetries = 2
    // STAR returns error code 104 when there is too little memory.
    errorStrategy = { task.exitStatus in [ 104, 137..140 ] ? 'retry' : 'finish' }

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

        starInputChannel =
            info2Channel
            .combine(fasta2Channel, by: 0)
            .combine(gtf2Channel, by: 0)
            .map
            {
                base, genomeInfo, fastaFile, gtfFile ->
                tuple genomeInfo, fastaFile, gtfFile
            }

        def processingCondition =
        {
            genomeInfo, fastaFile, gtfFile ->
            def starDir = "${assemblyPath(genomeInfo)}/star-${params.STAR_VERSION}"
            def requiredFiles = [ file("${starDir}/SA"), file("${starDir}/SAindex"), file("${starDir}/Genome") ]
            return requiredFiles.any { !it.exists() }
        }

        processingChoice = starInputChannel.branch
        {
            doIt: processingCondition(it)
            done: true
        }

        starIndex(processingChoice.doIt)

        presentChannel = processingChoice.done.map
        {
            genomeInfo, fastaFile, gtfFile ->
            tuple genomeInfo, file("${assemblyPath(genomeInfo)}/bwa-${params.BWA_VERSION}")
        }

        starChannel = presentChannel.mix(starIndex.out)

    emit:
        starChannel
}
