#!/bin/bash

java -Djava.io.tmpdir="$TMPDIR" \
-Xms!{javaMem}m -Xmx!{javaMem}m \
-cp !{params.REFBUILDER} \
org.cruk.pipelines.referencegenomes.ConcatenateFiles \
-o "!{outputFile}" \
!{inputFiles}
