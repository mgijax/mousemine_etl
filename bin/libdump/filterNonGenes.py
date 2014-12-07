#
# filterNonGenes.py
#
# Removes lines from Homologene or Panther files that refer to non-gene
# mouse markers. Reads from stdin, writes to stdout. 
#
# USAGE:
#	$ python filterNonGenes.py -t [homologene|panther] < INPUT > OUTPUT
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
Given the specified source (homologene or panther), loads the right IDs into 
the lookup cache, and returns a line testing function.
The function tests whether a given line should be written (True) or suppressed (False).
'''
def detectSource(src):
    if src == "homologene":
	# One gene per line, identified by EntrezGene id.
	ldb_keys = "55"
	def test(line,geneIds):
	    fields = line.split(TAB)
	    if fields[1] == '10090':
		id = fields[2]
		return id in geneIds
	    else:
	        return True

    elif src == "panther":
	# Panther. Two genes per line. Identified by MGI id or Ensembl id.
	ldb_keys = "1,60"
	def _test1(field, geneIds):
	    id = None
	    if field.startswith('MOUSE|MGI'):
		id = field.split(PIPE)[1].replace("MGI=MGI=", "MGI:")
	    elif field.startswith('MOUSE|Ensembl'):
		id = field.split(PIPE)[1].replace("Ensembl=","")
	    return id is None or id in geneIds

	def test(line,geneIds):
	    fields = line.split(TAB)
	    return _test1(fields[0],geneIds) or _test1(fields[1],geneIds)
    else:
        raise RuntimeError("Unrecognized input source.") 

    # load the id cache
    query = '''
	SELECT aa.accid
	FROM ACC_Accession aa, MRK_MCV_Cache mm
	WHERE  aa._mgitype_key = 2
	AND aa.private = 0
	AND aa._object_key = mm._marker_key
	AND mm.term = 'gene'
	AND _logicaldb_key in (%s)
	''' % ldb_keys
    db.sql(query, lambda r: geneIds.add(r['accid']))

    return test

def getCmdLine():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-t", "--type", dest="filetype",
                  help="File type. One of: homologene, panther")
    (options, args) = parser.parse_args()
    return (options,args)

def main():
    opts, args = getCmdLine()
    test = detectSource(opts.filetype)
    for line in sys.stdin:
	if test(line, geneIds):
	    sys.stdout.write(line)

main()
