#
# dumpEMAPAobo.py
#
# Script to dump the EMAPA vocabulary as an .obo file.
#

import sys
import mgiadhoc as db
import time

QTERMS = '''
    SELECT aa.accid as id, t._term_key, t.term as name, a.startstage, a.endstage
    FROM VOC_Term t, VOC_Term_EMAPA a, ACC_Accession aa
    WHERE t._term_key = a._term_key
    AND t._term_key = aa._object_key
    AND aa._logicaldb_key = 169
    AND aa._mgitype_key = 13
    AND aa.preferred = 1
    ORDER BY aa.accid
    '''

QALTIDS = '''
    SELECT t._term_key, aa.accid as id
    FROM VOC_Term t, ACC_Accession aa
    WHERE t._term_key = aa._object_key
    AND t._vocab_key = 90
    AND aa._logicaldb_key = 169
    AND aa._mgitype_key = 13
    AND aa.preferred = 0
    ORDER BY aa.accid
    '''

QSYNONYMS = '''
    SELECT t._term_key, t.term, s.synonym, st.synonymType
    FROM VOC_Term t, MGI_Synonym s, MGI_SynonymType st
    WHERE t._term_key = s._object_key
    AND s._mgitype_key = 13
    AND t._vocab_key = 90
    AND s._synonymtype_key = st._synonymtype_key
    '''

QEDGES = '''
    SELECT ct._term_key, 
	ct.term as child, 
	l.label,
	pt.term as parent,
	pa.accid as pid
    FROM voc_term pt, 
	dag_node pn, 
	dag_dag d, 
	voc_term ct, 
	dag_node cn, 
	dag_edge e, 
	dag_label l,
	acc_accession pa
    WHERE pt._vocab_key = 90
    AND d._mgitype_key = 13
    AND pt._term_key = pn._object_key
    AND ct._term_key = cn._object_key
    AND pn._dag_key = d._dag_key
    AND cn._dag_key = d._dag_key
    AND e._parent_key = pn._node_key
    AND e._child_key = cn._node_key
    AND e._label_key = l._label_key
    AND pt._term_key = pa._object_key
    AND pa._logicaldb_key = 169
    AND pa._mgitype_key = 13
    '''

# E.g.: 07:02:2014 16:28
DATE = time.strftime("%d:%m:%Y %H:%M", time.localtime(time.time()))

IHDR = '''format-version: 1.2
date: %s
saved-by: terryh
default-namespace: emapa
''' % DATE

ITERM = '''
[Term]
id: %(id)s
name: %(name)s
namespace: emapa
starts_at: %(startstage)s
ends_at: %(endstage)s
%(altids)s%(synonyms)s%(relationships)s
'''

NL = "\n"

ITAIL = '''
[Typedef]
id: is_a
name: is_a
is_transitive: true

[Typedef]
id: part_of
name: part_of
is_transitive: true

[Typedef]
id: ends_at
name: ends_at

[Typedef]
id: starts_at
name: starts_at

'''

class EmapaOboDumper:
    def __init__(self):
        self.ofd = sys.stdout

    def main(self):
	self.ofd.write(IHDR)
	#
	tid2parents = {}
	for e in db.sql(QEDGES):
	    l = e['label']
	    if l == "is-a":
		rel = "is_a: %(pid)s ! %(parent)s\n" % e
	    else:
		rel = "relationship: part_of %(pid)s ! %(parent)s\n" % e
	    tid2parents.setdefault(e['_term_key'],[]).append( rel )
	#
	tid2synonyms = {}
	for s in db.sql(QSYNONYMS):
	    syn = 'synonym: "%(synonym)s" RELATED []\n' % s
	    tid2synonyms.setdefault(s['_term_key'],[]).append( syn )
	    
	#
	tid2altids = {}
	for a in db.sql(QALTIDS):
	    aid = 'alt_id: %(id)s\n' % a
	    tid2altids.setdefault(a['_term_key'],[]).append(aid)

	#
        for r in db.sql(QTERMS):
	    r['relationships'] = ''.join(tid2parents.get(r['_term_key'],[]))
	    r['synonyms'] = ''.join(tid2synonyms.get(r['_term_key'],[]))
	    r['altids'] = ''.join(tid2altids.get(r['_term_key'],[]))
	    self.ofd.write( ITERM % r )

	#
	self.ofd.write(ITAIL)

	#
	self.ofd.close()

db.setConnectionDefaultsFromPropertiesFile()
EmapaOboDumper().main()

