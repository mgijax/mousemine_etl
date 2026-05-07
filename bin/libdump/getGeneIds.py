#
# getGeneIds.py
#
# Outputs a list of all the IDs for genes (MGI, Swiss-Prot, Ensembl, etc...)
# Other sources (panther, biogrid, intact) depend on the output of this source, 
# so this must be run before them
#

import os
import sys
import xml.etree.ElementTree as et
import mgidbconnect as db


TAB = '\t'
PIPE = '|'
NL = '\n'
suppressed = set()

def log (s) :
    sys.stderr.write(s + NL)

# Returns the set of all IDs for genes (MGI, SWISS-PROT, ENSEMBL, etc...)
def main():
    geneIds = set()
    query = '''
        SELECT aa.accid
        FROM ACC_Accession aa, MRK_MCV_Cache mm
        WHERE  aa._mgitype_key = 2
        AND aa.private = 0
        AND aa._object_key = mm._marker_key
        AND mm.term = 'gene'
        AND aa._logicaldb_key in (1,13,59,60,133,134)
        ''' 
    db.sql(query, lambda r: geneIds.add(r['accid']))
    for i in geneIds:
        sys.stdout.write(i + NL)

db.setConnectionFromPropertiesFile()
main()
