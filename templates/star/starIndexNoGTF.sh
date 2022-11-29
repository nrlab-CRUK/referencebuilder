#!/bin/bash

!{params.STAR} \
    --runMode genomeGenerate \
    --runThreadN !{task.cpus} \
    --limitGenomeGenerateRAM !{task.memory.bytes} \
    --genomeDir "!{indexDir}" \
    --genomeFastaFiles "!{fastaFile}" \
    --genomeSAindexNbases !{indexLength} \
    --outTmpDir temp
