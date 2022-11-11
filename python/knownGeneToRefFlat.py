#!/usr/bin/env python

# Quick and dirty script to convert a "knownGene" format file into refFlat format.
#
# Not quite simple enough to do in the shell, because we need to conditionally
# include one column if it is non-blank, otherwise a different column
#

import sys

GENE_ID_COL = 10
TRANSCRIPT_ID_COL = 11

count = 0
for line in sys.stdin:
  flds = line.strip().split("\t")
  if len(flds) != 12:
    sys.stderr.write("Bug: %d columns in line" % (count,))
    sys.exit(-1)
  if flds[GENE_ID_COL] == "":
    tag = flds[TRANSCRIPT_ID_COL]
  else:
    tag = flds[GENE_ID_COL]
  sys.stdout.write("%s\t%s\n" % (tag,"\t".join(flds[0:10])))
  count += 1
sys.stderr.write("translated %d lines\n" % (count,))
