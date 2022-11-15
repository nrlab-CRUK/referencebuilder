#!/bin/bash

# Convert a GenePred file to GTF format using UCSC's genePredToGtf tool.

set -euo pipefail

!{params.GENEPREDTOGTF} \
    !{database} !{table} \
    !{gtfFile}
