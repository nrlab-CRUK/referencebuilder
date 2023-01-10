/*
 * Pipeline to fetch and process blacklist files.
 * The blacklists are taken from the project https://github.com/Boyle-Lab/Blacklist
 */

include { assemblyPath } from '../functions/functions'

process fetch
{
    label 'fetcher'

    when:
        genomeInfo.containsKey('url.blacklist')

    input:
        val(genomeInfo)

    output:
        tuple val(genomeInfo), path(blacklistFile)

    shell:
        blacklistFile = "downloaded.gz"
        url = genomeInfo['url.blacklist']

        """
        wget -O !{blacklistFile} "!{url}"
        """
}

process uncompress
{
    label 'tiny'

    publishDir "${assemblyPath(genomeInfo)}/annotation", mode: 'copy'

    input:
        tuple val(genomeInfo), path(zippedFile)

    output:
        tuple val(genomeInfo), path(blacklistFile)

    shell:
        blacklistFile = "${genomeInfo.base}.blacklist.bed"

        """
        zcat "!{zippedFile}" > "!{blacklistFile}"
        """
}


workflow blacklistWF
{
    take:
        genomeInfoChannel

    main:
        blacklistChannel = genomeInfoChannel
            .filter
            {
                genomeInfo ->
                def blacklist = file("${assemblyPath(genomeInfo)}/annotation/${genomeInfo.base}.blacklist.bed")
                return !blacklist.exists()
            }

        fetch(blacklistChannel) | uncompress
}
