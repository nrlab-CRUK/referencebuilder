/*
 * Pipeline to fetch and process DBSNP files.
 */

include { assemblyPath } from '../functions/functions'

process fetch
{
    label 'fetcher'

    when:
        genomeInfo.containsKey("url.${type}" as String)

    input:
        tuple val(genomeInfo), val(type)

    output:
        tuple val(genomeInfo), val(type), path(dbsnpFile)

    shell:
        dbsnpFile = "downloaded.gz"
        url = genomeInfo["url.${type}" as String]

        """
        wget -O !{dbsnpFile} "!{url}"
        """
}

process recompress
{
    cpus 2

    publishDir "${assemblyPath(genomeInfo)}/dbsnp", mode: 'copy'

    input:
        tuple val(genomeInfo), val(type), path(dbsnpFile)

    output:
        tuple val(genomeInfo), val(type), path(zippedFile)

    shell:
        zippedFile = "${genomeInfo.base}.${type}.vcf.gz"

        """
        zcat "!{dbsnpFile}" | bgzip -c -l 9 > "!{zippedFile}"
        """
}

process index
{
    publishDir "${assemblyPath(genomeInfo)}/dbsnp", mode: 'copy'

    input:
        tuple val(genomeInfo), val(type), path(dbsnpFile)

    output:
        tuple val(genomeInfo), val(type), path("${dbsnpFile.name}.tbi")

    shell:
        """
        tabix "!{dbsnpFile}"
        """
}


workflow dbsnpWF
{
    take:
        genomeInfoChannel

    main:
        types = channel.of('snps', 'indels')

        fullChannel = genomeInfoChannel
            .combine(types)
            .filter
            {
                genomeInfo, type ->
                def dbsnpBase = "${assemblyPath(genomeInfo)}/dbsnp"
                def requiredFiles = [
                    "${dbsnpBase}/${genomeInfo.base}.${type}.vcf.gz",
                    "${dbsnpBase}/${genomeInfo.base}.${type}.vcf.gz.tbi",
                ]
                return requiredFiles.any { !file(it).exists() }
            }

        fetch(fullChannel) | recompress | index
}
