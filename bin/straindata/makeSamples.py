#
# makeSamples.py
#

import sys
import gff3lite as gff3

N = 1000

giter = gff3.iterate(sys.stdin)
header = giter.next()
for l in header:
    sys.stdout.write(l + '\n')
count = 0
for grp in giter:
    for f in grp:
        sys.stdout.write(gff3.formatLine(f) + '\n')
    sys.stdout.write(gff3.GFF3SEPARATOR  + '\n')
    count += 1
    if count > N:
        sys.exit(0)
