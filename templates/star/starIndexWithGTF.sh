#!/bin/bash

!{params.STAR} \
    --runMode genomeGenerate \
    --runThreadN !{task.cpus} \
    --limitGenomeGenerateRAM !{task.memory.bytes} \
    --genomeDir "!{indexDir}" \
    --genomeFastaFiles "!{fastaFile}" \
    --sjdbGTFfile "!{gtfFile}" \
    --genomeSAindexNbases !{indexLength} \
    --outTmpDir temp \
    --sjdbOverhang 100
