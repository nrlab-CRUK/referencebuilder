/*
 * Pipeline to fetch and process COSMIC files.
 */

include { assemblyPath; javaMemMB } from '../functions/functions'

// Coding mutants

process fetchMutants
{
    label 'fetcher'

    when:
        genomeInfo.containsKey('url.cosmicmutants')

    input:
        val(genomeInfo)

    output:
        tuple val(genomeInfo), path(mutantsFile)

    shell:
        mutantsFile = "downloaded.gz"

        """
        python3 "${projectDir}/python/fetchCosmic.py" \
            "!{genomeInfo['url.cosmicmutants']}" \
            "!{mutantsFile}"
        """
}

process recompressMutants
{
    cpus 2

    publishDir "${assemblyPath(genomeInfo)}/cosmic", mode: 'copy'

    input:
        tuple val(genomeInfo), path(mutantsFile)

    output:
        tuple val(genomeInfo), path(zippedFile)

    shell:
        zippedFile = "${genomeInfo.base}.codingmutants.vcf.gz"

        """
        zcat "!{mutantsFile}" | bgzip -c -l 9 > "!{zippedFile}"
        """
}

process indexMutants
{
    publishDir "${assemblyPath(genomeInfo)}/cosmic", mode: 'copy'

    input:
        tuple val(genomeInfo), path(mutantsFile)

    output:
        tuple val(genomeInfo), path("${mutantsFile.name}.tbi")

    shell:
        """
        tabix "!{mutantsFile}"
        """
}

// Non coding variants

process fetchVariants
{
    label 'fetcher'

    when:
        genomeInfo.containsKey('url.cosmicnoncoding')

    input:
        val(genomeInfo)

    output:
        tuple val(genomeInfo), path(variantsFile)

    shell:
        variantsFile = "downloaded.gz"

        """
        python3 "${projectDir}/python/fetchCosmic.py" \
            "!{genomeInfo['url.cosmicmutants']}" \
            "!{variantsFile}"
        """
}

process recompressVariants
{
    cpus 2

    publishDir "${assemblyPath(genomeInfo)}/cosmic", mode: 'copy'

    input:
        tuple val(genomeInfo), path(variantsFile)

    output:
        tuple val(genomeInfo), path(zippedFile)

    shell:
        zippedFile = "${genomeInfo.base}.noncodingvariants.vcf.gz"

        """
        zcat "!{variantsFile}" | bgzip -c -l 9 > "!{zippedFile}"
        """
}

process indexVariants
{
    publishDir "${assemblyPath(genomeInfo)}/cosmic", mode: 'copy'

    input:
        tuple val(genomeInfo), path(variantsFile)

    output:
        tuple val(genomeInfo), path("${variantsFile.name}.tbi")

    shell:
        """
        tabix "!{variantsFile}"
        """
}


workflow cosmicWF
{
    take:
        genomeInfoChannel

    main:
        mutantsChannel = genomeInfoChannel
            .filter
            {
                genomeInfo ->
                def cosmicBase = "${assemblyPath(genomeInfo)}/cosmic"
                def requiredFiles = [
                    "${cosmicBase}/${genomeInfo.base}.codingmutants.vcf.gz",
                    "${cosmicBase}/${genomeInfo.base}.codingmutants.vcf.gz.tbi",
                ]
                return requiredFiles.any { !file(it).exists() }
            }

        fetchMutants(mutantsChannel) | recompressMutants | indexMutants

        variantsChannel = genomeInfoChannel
            .filter
            {
                genomeInfo ->
                def cosmicBase = "${assemblyPath(genomeInfo)}/cosmic"
                def requiredFiles = [
                    "${cosmicBase}/${genomeInfo.base}.noncodingvariants.vcf.gz",
                    "${cosmicBase}/${genomeInfo.base}.noncodingvariants.vcf.gz.tbi",
                ]
                return requiredFiles.any { !file(it).exists() }
            }

        fetchVariants(variantsChannel) | recompressVariants | indexVariants
}
