#
# filterProtein2Ipr.py
#
# Filter the protein2ipr data file for a specified set of protein ids.
#
# This filter takes a single command line argument, which is the file of IDs to filter for,
# It then reads the protein2ipr data from stdin and writes to stadout only those lines where
# the protein is listed in the file. E.g.:
#       % zcat protein2ipr.dat.gz | python filterProtein2Ipr.py idFile.dat > filteredData.dat
#
# Why:
# The file containing interpro-to-protein domain data is over 3 GB compressed.
# Not even sure how big it is uncompressed because I keep exceeding my disk quota.
# It's so big because it contains data for ALL organisms. We only need mouse, which
# is a tiny fraction. That's what this filter is for.
# 
import os
import sys

def readIdFile(fname):
    # text file, one ID per line
    idset = set()
    fd = open(fname, 'r')
    for line in fd:
        idset.add(line.strip())
    fd.close()
    return idset

def main(idfile):
    idset = readIdFile(idfile)
    for line in sys.stdin:
        id,rest = line.split('\t', 1)
        if id in idset:
            sys.stdout.write(line)

main(sys.argv[1])
