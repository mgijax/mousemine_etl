#
# getMgiSecondaryIds.py
# 
# Gets all secondary ids and their corresponding primary ids, and writes them to
# a 2 column tab delimited file.
#

import sys
import urllib

q = '''<query
    name="" model="genomic"
    view="SequenceFeature.synonyms.value SequenceFeature.primaryIdentifier"
    longDescription=""
    sortOrder="SequenceFeature.primaryIdentifier asc"
    constraintLogic="A and B">
    <constraint path="SequenceFeature.primaryIdentifier" code="A" op="CONTAINS" value="MGI:"/>
    <constraint path="SequenceFeature.synonyms.value" code="B" op="CONTAINS" value="MGI:"/>
</query>'''

url = 'http://www.mousemine.org/mousemine/service/query/results?query=' + urllib.quote_plus(q);
fd = urllib.urlopen(url)
sys.stdout.write("secondaryId\tprimaryId\n")
sys.stdout.write(fd.read())
