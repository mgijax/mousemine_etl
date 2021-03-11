#
# getMgiSecondaryIds.py
# 
# Queries MouseMine for all mouse feature primary and secondary ids.
# Writes a 2-column file of primaryId, secondaryId
#

import sys
from urllib.parse import quote_plus
from urllib.request import urlopen
import mgidbconnect as db

db.setConnectionFromPropertiesFile()

q = '''
    SELECT 
      aa.accid as mgiid,
      aa2.accid as modelid,
      aa2._logicaldb_key
    FROM
      acc_accession aa,
      acc_accession aa2
    WHERE aa._logicaldb_key = 1
      AND aa._mgitype_key = 2
      AND aa.private = 0
      AND aa.preferred = 1
      AND aa._object_key = aa2._object_key
      AND aa2._logicaldb_key = 60
      AND aa2._mgitype_key = 2
      AND aa2.preferred = 1
      '''
sys.stdout.write("ensembl\tmgi\n")
for r in db.sql(q):
    sys.stdout.write('%s\t%s\n' % (r['modelid'], r['mgiid']))

