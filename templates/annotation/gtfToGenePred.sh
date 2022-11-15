#!/bin/bash

# Convert a GTF file to RefFlat format using UCSC's gtfToGenePred tool.

set -euo pipefail

!{params.GTFTOGENEPRED} \
    -infoOut=info.txt \
    !{gtfFile} \
    refflat.txt

paste \
    <(tail -n +2 info.txt | cut -f 2) \
    <(cut -f 1-10 refflat.txt) \
    > !{refFlatFile}
