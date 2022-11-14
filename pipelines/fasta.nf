include { assemblyPath; javaMemMB } from '../functions/functions'
include { maxReadsInRam } from '../functions/picard'

process fetchFasta
{
    memory '4MB'

    input:
        val(genomeInfo)

    output:
        tuple val(genomeInfo), path(fastaFile)

    shell:
        fastaFile = "downloaded.fa.gz"

        """
        curl -s -o !{fastaFile} "!{genomeInfo['url.fasta']}"
        """
}

process recreateFasta
{
    publishDir "${assemblyPath(genomeInfo)}/fasta", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path(correctedFile)

    shell:
        javaMem = javaMemMB(task)
        correctedFile = "${genomeInfo.base}.fa"

        template "fasta/RecreateFasta.sh"
}

process indexFasta
{
    publishDir "${assemblyPath(genomeInfo)}/fasta", mode: 'copy', pattern: '*.fai'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path(fastaFile), path(indexFile)

    shell:
        indexFile = fastaFile.name + ".fai"

        """
        !{params.SAMTOOLS} faidx !{fastaFile}
        """
}


/*
 * Run Picard's 'CreateSequenceDictionary'.
 */
process sequenceDictionary
{
    label "picard"

    publishDir "${assemblyPath(genomeInfo)}/fasta", mode: 'copy'

    input:
        tuple val(genomeInfo), path(fastaFile)

    output:
        tuple val(genomeInfo), path(fastaFile), path(sequenceDictionary)

    shell:
        javaMem = javaMemMB(task)
        sequenceDictionary = "${genomeInfo.base}.dict"

        template "picard/CreateSequenceDictionary.sh"
}

/*
    Create a sequence/chromosome sizes file (used by UCSC bedToBigBed utility).
 */
process sizesFile
{
    memory '4MB'

    publishDir "${assemblyPath(genomeInfo)}/fasta", mode: 'copy', pattern: '*.sizes'

    input:
        tuple val(genomeInfo), path(fastaFile), path(sequenceDictionary)

    output:
        tuple val(genomeInfo), path(fastaFile), path(sizesFile)

    shell:
        sizesFile = "${genomeInfo.base}.sizes"

        """
            set -euo pipefail
            grep "^@SQ" !{sequenceDictionary} | \
                cut -f2,3 | \
                sed 's/^SN://;s/\tLN:/\t/' \
                > !{sizesFile}
        """
}

/*
    Create 'canonical' chromosomes file, that is, the normal chromosomes, not
    including, for example, "alt" or "unplaced" chromosomes.  The simple rule
    of removing chromosomes/contigs with an underscore or period works fine for
    most of the species currently in use, but produces silly results for others.
    Unfortunately, short of manual curation, there doesn't appear to be a simple
    rule that works for all.  Possibly we could provide a regular expression
    in the genome metadata file if that seems useful.
 */
process canonicalChromosomes
{
    memory '4MB'

    publishDir "${assemblyPath(genomeInfo)}/fasta", mode: 'copy', pattern: '*.canonical'

    input:
        tuple val(genomeInfo), path(fastaFile), path(sizesFile)

    output:
        tuple val(genomeInfo), path(fastaFile), path(canonicalFile)

    shell:
        canonicalFile = "${genomeInfo.base}.canonical"

        """
            set -euo pipefail
            sed -n -e '/[_.]/ !p' \
                < !{sizesFile} \
                | cut -f 1 \
                > !{canonicalFile}
        """
}

workflow fastaWF
{
    take:
        genomeInfoChannel

    main:

        def processingCondition =
        {
            genomeInfo ->
            def requiredFiles = [
                file("${assemblyPath(genomeInfo)}/fasta/${genomeInfo.base}.fa"),
                file("${assemblyPath(genomeInfo)}/fasta/${genomeInfo.base}.fa.fai"),
                file("${assemblyPath(genomeInfo)}/fasta/${genomeInfo.base}.dict"),
                file("${assemblyPath(genomeInfo)}/fasta/${genomeInfo.base}.sizes"),
                file("${assemblyPath(genomeInfo)}/fasta/${genomeInfo.base}.canonical")
            ]
            return requiredFiles.any { !it.exists() }
        }

        processingChoice = genomeInfoChannel.branch
        {
            doIt: processingCondition(it)
            done: true
        }

        fetchFasta(processingChoice.doIt) | recreateFasta

        indexFasta(recreateFasta.out)

        sequenceDictionary(recreateFasta.out) | sizesFile | canonicalChromosomes

        presentChannel = processingChoice.done.map
        {
            genomeInfo ->
            tuple genomeInfo, file("${assemblyPath(genomeInfo)}/fasta/${genomeInfo.base}.fa")
        }

        fastaChannel = presentChannel.mix(recreateFasta.out)

    emit:
        fastaChannel
}
