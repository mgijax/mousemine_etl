
from AbstractItemDumper import *
import itertools


class RelationshipDumper(AbstractItemDumper):
    q0 = '''
SELECT c._category_key, c.name, st.name as stype, ot.name as otype
FROM MGI_Relationship_Category c, ACC_MGItype st, ACC_MGIType ot
WHERE c._mgitype_key_1 = st._mgitype_key
AND c._mgitype_key_2 = ot._mgitype_key
    '''
    q1 = '''
SELECT 
    r._relationship_key,
    r._category_key,
    r._object_key_1,
    r._object_key_2,
    r._relationshipterm_key,
    q.term as qualifier,
    e.abbreviation as evidencecode,
    r._refs_key
FROM  MGI_Relationship r, VOC_Term q, VOC_Term e
WHERE r._category_key = %d
AND r._qualifier_key = q._term_key
AND r._evidence_key = e._term_key
ORDER BY r._relationship_key
    '''
    q2 = '''
SELECT p._relationship_key, t.term as property, p.value
FROM MGI_Relationship_Property p, VOC_Term t
WHERE p._propertyname_key = t._term_key
AND p._relationship_key in (
  SELECT r._relationship_key
  FROM  MGI_Relationship r
  WHERE r._category_key = %d
  )
ORDER BY p._relationship_key, p.sequenceNum
    '''
    rtmplt = '''
<item class="DirectedRelationship" id="%(id)s">
<reference name="subject" ref_id="%(subject)s" />
<reference name="object" ref_id="%(object)s" />
<reference name="relationshipTerm" ref_id="%(relationshipterm)s" />
%(qualifier)s<attribute name="evidenceCode" value="%(evidencecode)s" />
%(propertystring)s<reference name="publication" ref_id="%(publication)s" />
<reference name="dataSet" ref_id="%(dataset)s" />
</item>
    '''
    ptmplt = '''
<item class="DirectedRelationshipProperty" id="%(id)s">
<attribute name="property" value="%(property)s" />
<attribute name="value" value="%(value)s" />
<reference name="relationship" ref_id="%(relationship)s" />
</item>
    '''

    def preDump(self):
	self.categories = {}
	for c in self.context.sql(self.q0):
	    self.categories[c['_category_key']] = c

    def mergeIter(self):
	atk = 1004
	q1 = RelationshipDumper.q1 % atk
	q2 = RelationshipDumper.q2 % atk
	i1 = itertools.groupby(self.context.sqliter(q1), lambda r:r['_relationship_key'])
	i2 = itertools.groupby(self.context.sqliter(q2), lambda r:r['_relationship_key'])

	try:
	    r1 = None
	    k1,r1 = i1.next()
	    k2,r2 = i2.next()
	    while True:
		if k1 < k2:
		    yield (list(r1)[0],[])
		    k1,r1 = i1.next()
		elif k1 > k2:
		    # should never happen
		    raise RuntimeException()
		else:
		    yield (list(r1)[0],list(r2))
		    r1 = None
		    k1,r1 = i1.next()
		    k2,r2 = i2.next()

	except StopIteration:
	    if r1:
	        (list(r1)[0],[])
		for k1,r1 in i1:
		    yield (list(r1)[0],[])

    def mainDump(self):
	for rel, props in self.mergeIter():
	    rel['id'] = self.context.makeItemId('DirectedRelationship', rel['_relationship_key'])
	    c = self.categories[rel['_category_key']]
	    rel['subject'] = self.context.makeItemRef(c['stype'], rel['_object_key_1'])
	    rel['object'] = self.context.makeItemRef(c['otype'], rel['_object_key_1'])
	    if rel['qualifier'] == "Not Specified":
	        rel['qualifier'] = ''
	    else:
	        rel['qualifier'] = '<attribute name="qualifier" value="%s" />\n'%rel['qualifier']
	    rel['relationshipterm'] = self.context.makeItemRef('Vocabulary Term', rel['_relationshipterm_key'])
	    rel['publication'] = self.context.makeItemRef('Reference', rel['_refs_key'])
	    ps = " ; ".join(map(lambda r:"%(property)s=%(value)s"%r, props))
	    if len(ps)>0:
		rel['propertystring'] = '<attribute name="propertyString" value="%s" />\n' % ps
	    else:
	        rel['propertystring'] = ''
	    rel['dataset'] = "???"
	    self.writeItem(rel, self.rtmplt)
	    for p in props:
	        p['id'] = self.context.makeItemId('DirectedRelationshipProperty')
		p['relationship'] = self.context.makeItemRef('DirectedRelationship', p['_relationship_key'])
		self.writeItem(p, self.ptmplt)

#

