#
# getMgiSecondaryIds.py
# 
# Queries MouseMine for all mouse feature primary and secondary ids.
# Writes a 2-column file of primaryId, secondaryId
#

import sys
import urllib

q = '''<query 
    model="genomic"
    view="SequenceFeature.crossReferences.identifier SequenceFeature.primaryIdentifier SequenceFeature.symbol"
    sortOrder="SequenceFeature.primaryIdentifier ASC"
    >
	<constraint path="SequenceFeature.crossReferences.source.name" op="=" value="Ensembl Gene Model" code="A" />
    </query>'''

url = 'http://www.mousemine.org/mousemine/service/query/results?query=' + urllib.quote_plus(q);
fd = urllib.urlopen(url)
sys.stdout.write("ensembl\tmgi\n")
for line in fd:
    toks = line[:-1].split('\t')
    toks[0] = toks[0].split('.')[0]
    sys.stdout.write('%s\t%s\n' % (toks[0], toks[1]))
