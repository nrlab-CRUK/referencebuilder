#!/bin/bash

mkdir "!{indexDir}"
cd "!{indexDir}"

!{params.STAR} \
    --runMode genomeGenerate \
    --runThreadN !{task.cpus} \
    --limitGenomeGenerateRAM !{task.memory.toKilo()} \
    --genomeDir "!{indexDir}" \
    --genomeFastaFiles "../!{fastaFile}" \
    --sjdbGTFfile "../!{gtfFile}" \
    --genomeSAindexNbases !{indexLength} \
    --outTmpDir ../temp \
    --sjdbOverhang 100
