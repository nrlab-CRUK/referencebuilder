/*
 * Pipeline to fetch and process DBSNP files.
 */

include { assemblyPath; javaMemMB } from '../functions/functions'

// SNP files

process fetchSnps
{
    label 'fetcher'

    when:
        genomeInfo.containsKey('url.snps')

    input:
        val(genomeInfo)

    output:
        tuple val(genomeInfo), path(snpFile)

    shell:
        snpFile = "downloaded.gz"

        """
        wget -O !{snpFile} "!{genomeInfo['url.snps']}"
        """
}

process recompressSnps
{
    cpus 2

    publishDir "${assemblyPath(genomeInfo)}/dbsnp", mode: 'copy'

    input:
        tuple val(genomeInfo), path(snpFile)

    output:
        tuple val(genomeInfo), path(zippedFile)

    shell:
        zippedFile = "${genomeInfo.base}.snps.vcf.gz"

        """
        zcat "!{snpFile}" | bgzip -c -l 9 > "!{zippedFile}"
        """
}

process indexSnps
{
    publishDir "${assemblyPath(genomeInfo)}/dbsnp", mode: 'copy'

    input:
        tuple val(genomeInfo), path(snpFile)

    output:
        tuple val(genomeInfo), path("${snpFile.name}.tbi")

    shell:
        """
        tabix "!{snpFile}"
        """
}

// Indel files

process fetchIndels
{
    label 'fetcher'

    when:
        genomeInfo.containsKey('url.indels')

    input:
        val(genomeInfo)

    output:
        tuple val(genomeInfo), path(indelFile)

    shell:
        indelFile = "downloaded.blob"

        """
        wget -O !{indelFile} "!{genomeInfo['url.indels']}"
        """
}

process recompressIndels
{
    cpus 2

    publishDir "${assemblyPath(genomeInfo)}/dbsnp", mode: 'copy'

    input:
        tuple val(genomeInfo), path(indelFile)

    output:
        tuple val(genomeInfo), path(zippedFile)

    shell:
        zippedFile = "${genomeInfo.base}.indels.vcf.gz"

        """
        zcat "!{indelFile}" | bgzip -c -l 9 > "!{zippedFile}"
        """
}

process indexIndels
{
    publishDir "${assemblyPath(genomeInfo)}/dbsnp", mode: 'copy'

    input:
        tuple val(genomeInfo), path(indelFile)

    output:
        tuple val(genomeInfo), path("${indelFile.name}.tbi")

    shell:
        """
        tabix "!{indelFile}"
        """
}


workflow dbsnpWF
{
    take:
        genomeInfoChannel

    main:
        snpsChannel = genomeInfoChannel
            .filter
            {
                genomeInfo ->
                def dbsnpBase = "${assemblyPath(genomeInfo)}/dbsnp"
                def requiredFiles = [
                    "${dbsnpBase}/${genomeInfo.base}.snps.vcf.gz",
                    "${dbsnpBase}/${genomeInfo.base}.snps.vcf.gz.tbi",
                ]
                return requiredFiles.any { !file(it).exists() }
            }

        fetchSnps(snpsChannel) | recompressSnps | indexSnps

        indelsChannel = genomeInfoChannel
            .filter
            {
                genomeInfo ->
                def dbsnpBase = "${assemblyPath(genomeInfo)}/dbsnp"
                def requiredFiles = [
                    "${dbsnpBase}/${genomeInfo.base}.indels.vcf.gz",
                    "${dbsnpBase}/${genomeInfo.base}.indels.vcf.gz.tbi",
                ]
                return requiredFiles.any { !file(it).exists() }
            }

        fetchIndels(indelsChannel) | recompressIndels | indexIndels
}
