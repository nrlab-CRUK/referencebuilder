/*
 * Pipeline to fetch and process COSMIC files.
 */

include { assemblyPath } from '../functions/functions'

// Map from the URL key part in properties to the part of the filename.
def filenamePart(type)
{
    switch (type)
    {
        case 'cosmicmutants':   return 'codingmutants'
        case 'cosmicnoncoding': return 'noncodingvariants'
        default: throw new IllegalArgumentException("${type} is not an expected cosmic type.")
    }
}

process fetch
{
    label 'fetcher'

    when:
        genomeInfo.containsKey("url.${type}" as String)

    input:
        tuple val(genomeInfo), val(type)

    output:
        tuple val(genomeInfo), val(type), path(cosmicFile)

    shell:
        cosmicFile = "downloaded.gz"
        url = genomeInfo["url.${type}" as String]

        """
        python3 "${projectDir}/python/fetchCosmic.py" \
            "!{url}" \
            "!{cosmicFile}"
        """
}

process recompress
{
    cpus 2

    publishDir "${assemblyPath(genomeInfo)}/cosmic", mode: 'copy'

    input:
        tuple val(genomeInfo), val(type), path(cosmicFile)

    output:
        tuple val(genomeInfo), val(type), path(zippedFile)

    shell:
        zippedFile = "${genomeInfo.base}.${filenamePart(type)}.vcf.gz"

        """
        zcat "!{cosmicFile}" | bgzip -c -l 9 > "!{zippedFile}"
        """
}

process index
{
    publishDir "${assemblyPath(genomeInfo)}/cosmic", mode: 'copy'

    input:
        tuple val(genomeInfo), val(type), path(cosmicFile)

    output:
        tuple val(genomeInfo), val(type), path("${cosmicFile.name}.tbi")

    shell:
        """
        tabix "!{cosmicFile}"
        """
}


workflow cosmicWF
{
    take:
        genomeInfoChannel

    main:
        types = channel.of('cosmicmutants', 'cosmicnoncoding')

        fullChannel = genomeInfoChannel
            .combine(types)
            .filter
            {
                genomeInfo, type ->
                def cosmicBase = "${assemblyPath(genomeInfo)}/cosmic"
                def requiredFiles = [
                    "${cosmicBase}/${genomeInfo.base}.${filenamePart(type)}.vcf.gz",
                    "${cosmicBase}/${genomeInfo.base}.${filenamePart(type)}.vcf.gz.tbi",
                ]
                return requiredFiles.any { !file(it).exists() }
            }

        fetch(fullChannel) | recompress | index
}
