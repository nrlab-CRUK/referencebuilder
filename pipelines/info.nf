/*
 * Pipeline to put a copy of the genome info properties into the
 * assembly directory.
 */

include { assemblyPath; readGenomeInfo } from '../functions/functions'

process copyGenomeInfoFile
{
    label 'tiny'

    publishDir "${assemblyPath(genomeInfo)}", mode: 'copy'

    input:
        tuple val(genomeInfo), path(genomeInfoFile)

    output:
        tuple val(genomeInfo), path(infoFileName)

    shell:
        infoFileName = 'AssemblyInfo.properties'
        """
        cp "!{genomeInfoFile}" "!{infoFileName}"
        """
}

workflow genomeInfoWF
{
    take:
        genomeInfoFileChannel

    main:

        genomeInfoChannel = genomeInfoFileChannel
            .map
            {
                tuple readGenomeInfo(it), it
            }
            .filter
            {
                genomeInfo, genomeInfoFile ->
                def localFile = file("${assemblyPath(genomeInfo)}/AssemblyInfo.properties")
                return !localFile.exists()
            }

        copyGenomeInfoFile(genomeInfoChannel)
}
