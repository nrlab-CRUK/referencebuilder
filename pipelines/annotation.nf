/*
    Pipeline to fetch and prepare annotation files.

    Fetches the file from one of three source formats (GTF, KnownGene or EnsGene)
    and produces both a GTF file and a RefFlat file for annotation regardless of
    the source format.
 */

 process fetchGtf
 {
     memory '4MB'

     input:
         val(genomeInfo)

     output:
         tuple val(genomeInfo), path(gtfFile)

     shell:
         gtfFile = "downloaded.gtf"

         """
         curl -s -o !{gtfFile} "!{genomeInfo['url.gtf']}"
         """
 }
