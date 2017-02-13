#
# filterEntrez.py
#
# USAGE:
#	$ python filterEntrez.py < INPUT > OUTPUT
# where INPUT is the gene file downloaded from NCBI and OUTPUT is the corrected file.
#
# WHY we need it:
#
# The entrez file plays a central role in the Intermine data integration process.
# Specifically, it is used as to map common gene identifiers (e.g. NCBI_Gene ids) to their
# MOD equivalents. For example, Homologene data uses NCBI Gene ids. The entrez file is used to
# map these ids to MGI: ids for the purpose of integration.
# The problem is that the entez file has a lot of errors (at least, from MGI's perspective).
# There are thousands of cases where the entrez file maps an NCBI id to an MGI gene where we
# do not. Or the mapping in the entrez file is to the wrong gene. Or they don't have an MGI:
# mapping when they should. Or they have the MGI id matched with the wrong Vega id. Or...
# For all these reasons, this script exists. 
#
# WHAT IT DOES:
# In a nutshell, it replaces the contents of column 5 for mouse genes in the entrez file with
# MGI's version of the truth. 
#
# The entrez file is a simple tab-delimited format. There are 3 columns that concern us:
# Column 0 is the taxon id. This script only touches lines where column 0 is "10090" (mouse);
# other lines pass through unchanged. 
# Column 1 is the NCBI Gene id. The file has one line per id.
# Column 5 is the xrefs for the gene. For a mouse gene, column 5
# contains the MGI, Ensembl, and Vega ids that correspond to the NCBI id in column 1.
# This script ensures that the mapping from NCBI id to MGI/Ensembl/Vega ids agrees with MGI.
# If there is no mapping for an NCBI id, column 5 contains "-".
#
# This script reads from stdin and writes to stdout. 
# For each line changed, writes a triplet to stderr: the ncbi id, the original column 5, the corrected column 5.
#

import os
import sys
import logging
import xml.etree.ElementTree as et
import mgiadhoc as db


TAB = '\t'
PIPE = '|'
NL = '\n'

def cacheIdsN():
  # cache ids from NCBI, mapped to MGI ids
  n2m = {}
  q = '''
  SELECT aa2.accid AS mgiid, aa.accid AS ncbiid
  FROM ACC_Accession aa, ACC_Accession aa2
  WHERE aa._logicaldb_key in (55,59,160)
  AND aa._mgitype_key = 2
  AND aa._object_key = aa2._object_key
  AND aa2._mgitype_key = 2
  AND aa2._logicaldb_key = 1
  AND aa2.preferred = 1
  '''
  for r in db.sql(q):
    n2m[r['ncbiid']] = 'MGI:' + r['mgiid']
  return n2m

def cacheIdsEV():
  #  cache ids from Ensembl and Vega, mapped to MGI ids
  m2ev = {}
  q = '''
  SELECT aa.accid AS mgiid, aa2.accid
  FROM ACC_Accession aa, ACC_Accession aa2
  WHERE aa._logicaldb_key = 1
  AND aa._mgitype_key = 2
  AND aa.preferred = 1
  AND aa._object_key = aa2._object_key
  AND aa2._mgitype_key = 2
  AND aa2._logicaldb_key in (60,85)
  '''
  for r in db.sql(q):
    id = r['accid']
    if id.startswith('ENS'):
      id = 'Ensembl:'+id
    else:
      id = 'Vega:'+id
    m2ev.setdefault('MGI:'+r['mgiid'],[]).append(id)
  return m2ev

def formatMgiIds(ncbiId, n2m, m2ev):
  if ncbiId not in n2m:
    return '-'
  mgiId = n2m[ncbiId]
  if mgiId not in m2ev:
    return mgiId
  evIds = m2ev[mgiId]
  evIds.sort()
  return PIPE.join([mgiId] + evIds)

def main():
  n2m = cacheIdsN()
  m2ev = cacheIdsEV()
  for line in sys.stdin:
    fields = line.split(TAB)
    if fields[0] != "10090":
      sys.stdout.write(line)
    else:
      ncbiId = fields[1]
      mgiIds = formatMgiIds(ncbiId, n2m, m2ev)
      if mgiIds != fields[5]:
        sys.stderr.write('Changed: %s\nFrom: %s\nTo:   %s\n' % (ncbiId, fields[5], mgiIds))
      fields[5] = mgiIds
      line2 = TAB.join(fields)
      sys.stdout.write(line2)

#
main()


