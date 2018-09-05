#
# gff3lite.py
#

import types

# Character constants
TAB	= '\t'
NL	= '\n'
SEMI	= ';'
EQ	= '='
HASH	= '#'
BANG	= '!'
COMMA	= ','
# GFF3 field index constants
SEQID	= 0
SOURCE	= 1
TYPE	= 2
START	= 3
END	= 4
SCORE	= 5
STRAND	= 6
PHASE	= 7
ATTRIBUTES = 8
#
GFF3HEADER = "##gff-version 3"
GFF3SEPARATOR = "###"
#
def parseCol9(s):
    parts = dict([ part.split(EQ,1) for part in s.split(SEMI)])
    return parts
#
def parseLine(line):
    f = line.split(TAB)
    f[8] = parseCol9(f[8])
    return f
#
def formatCol9(attrs):
    s = SEMI.join(['%s=%s'%(k,v) for (k,v) in attrs.items()])
    return s
#
def formatLine(f):
    f2 = list(f)
    f2[8] = formatCol9(f2[8])
    return TAB.join([str(x) for x in f2])

def iterate(fileIn, yieldHeader=True, yieldGroups=True):
    # open file
    if type(fileIn) is types.StringType:
        fin = open(fileIn, 'r')
    else:
        fin = fileIn
    # collect header lines
    header = []
    line = fin.next()
    while line.startswith(HASH):
        header.append(line[:-1])
	line = fin.next()
    # yield the header, if requested
    if yieldHeader:
        yield header
    # main loop
    currGroup = []
    while line:
        line = line[:-1]
	if line == GFF3SEPARATOR:
	    if len(currGroup) > 0:
		if yieldGroups:
		    yield currGroup
		else:
		    for f in currGroup:
		        yield f
		currGroup = []
	else:
	    currGroup.append(parseLine(line))
	line = fin.next()
    fin.close()
