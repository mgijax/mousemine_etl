#
# filterNonGenes.py
#
# Removes lines from Homologene or Panther files that refer to non-gene
# mouse markers. Reads from stdin, writes to stdout. The script autodetects
# which source (Homologene or Panther) the input comes from.
#
# USAGE:
#	$ python filterNonGenes.py < INPUT > OUTPUT
#
# Why:
# Homologene and Panther data files contain some entries for non-gene 
# features such as gene segments and pseudogenes. This is legitimate data,
# however, the current load software does not handle them well. (Since it
# only looks at Genes, it doesn't find them in the db. So it goes ahead
# and creates new Gene objects with duplicate ids.)
#
#

import sys
import logging
import mgiadhoc as db

TAB = '\t'
PIPE = '|'
NL = '\n'
geneIds = set()
suppressed = set()

'''
detectSource:
Given a line from a data file, determines which source it is from and sets up
for filtering that source. 
Side effects: loads the right kind of ids (e.g., MGI or EntrezGene) for all genes
and MCV subtypes into a cache.
Returns: a function to test whether a given line should be written (True) or suppressed (False).
'''
def detectSource(line):
    fs = line.split(TAB)
    if len(fs) == 6:
	# Homologene. One gene per line, identified by EntrezGene id.
	src = "homologene"
	query = '''
	SELECT aa.accid
	FROM ACC_Accession aa, MRK_MCV_Cache mm
	WHERE  aa._mgitype_key = 2
	AND aa.private = 0
	AND aa._object_key = mm._marker_key
	AND mm.term = 'gene'
	AND _logicaldb_key = 55
	'''
	def test(line,geneIds):
	    fields = line.split(TAB)
	    if fields[1] == '10090':
		id = fields[2]
		return id in geneIds
	    else:
	        return True

    elif len(fs) == 5:
	# Panther. Two genes per line. Identified by MGI id.
        src =  "panther"
	query = '''
	SELECT aa.accid
	FROM ACC_Accession aa, MRK_MCV_Cache mm
	WHERE aa._mgitype_key = 2
	AND aa.private = 0
	AND aa._object_key = mm._marker_key
	AND mm.term = 'gene'
	AND aa._logicaldb_key = 1
	AND aa.prefixPart = 'MGI:'
	'''
	def test(line,geneIds):
	    fields = line.split(TAB)
	    if fields[0].startswith('MOUSE|MGI'):
		id = fields[0].split(PIPE)[1].replace("MGI=MGI=", "MGI:")
		if id not in geneIds:
		    return False
	    if fields[1].startswith('MOUSE|MGI'):
		id = fields[1].split(PIPE)[1].replace("MGI=MGI=", "MGI:")
		if id not in geneIds:
		    return False
	    return True
    else:
        raise RuntimeError("Unrecognized input source.") 

    db.sql(query, lambda r: geneIds.add(r['accid']))
    return test

def main():
    line = sys.stdin.readline()
    test = detectSource(line)
    while line:
	if test(line, geneIds):
	    sys.stdout.write(line)
	line = sys.stdin.readline()

main()
