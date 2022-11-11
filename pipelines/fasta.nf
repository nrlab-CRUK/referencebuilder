include { assemblyPath; filenameRoot } from '../functions/functions'
include { CreateSequenceDictionary as sequenceDictionary } from '../processes/picard'

process fetchFasta
{
    memory '4MB'
    
    input:
        tuple val(id), val(genomeInfo)

    output:
        tuple val(id), val(genomeInfo), path(fastaFile)
                
    shell:
        fastaFile = filenameRoot(genomeInfo) + ".fa.gz"
        
        """
        curl -s -o !{fastaFile} "!{genomeInfo['url.fasta']}"
        """
}

// This one will recreate the file, but for now just uncompress it.
process recreateFasta
{
    memory '4MB'
    
    input:
        tuple val(id), val(genomeInfo), path(fastaFile)
    
    output:
        tuple val(id), val(genomeInfo), path(uncompressedFile)
    
    shell:
        uncompressedFile = filenameRoot(genomeInfo) + ".fa"
        
        """
        zcat !{fastaFile} > !{uncompressedFile}
        """

}

process indexFasta
{
    publishDir "${assemblyPath(genomeInfo)}/fasta", mode: 'copy'
    
    input:
        tuple val(id), val(genomeInfo), path(fastaFile)

    output:
        tuple val(id), val(genomeInfo), path(fastaFile), emit: fasta
        path(indexFile)

    shell:
        indexFile = fastaFile.name + ".fai"
        
        """
        !{params.SAMTOOLS} faidx !{fastaFile}
        """
}

workflow fastaWF
{
    take:
        genomeInfoChannel
        
    main:
        fetchFasta(genomeInfoChannel) | recreateFasta
        
        indexFasta(recreateFasta.out)
        
        sequenceDictionary(recreateFasta.out)

    emit:
        fetchFasta.out
}
