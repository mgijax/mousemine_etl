#
# getValidMgiIds.py
# 
# Gets the current set of valid MGI ids for SequenceFeature
#

import sys
import urllib

q = '''
<query model="genomic" view="SequenceFeature.primaryIdentifier" >
    <constraint path="SequenceFeature.dataSets.name" op="=" value="Mouse Gene Catalog from MGI" code="A" />
</query>'''

url = 'http://www.mousemine.org/mousemine/service/query/results?query=' + urllib.quote_plus(q);
fd = urllib.urlopen(url)
sys.stdout.write(fd.read())
