
from AbstractItemDumper import *
from DataSourceDumper import DataSetDumper
import itertools

# Translation of "Non-mouse organism" values into taxon ids.
# This really needs to go somewhere better, but where??  FIXME.
# For now, take the simplest literal approach - define mapping for all known variants.
ORGANISM_NAMES = '''
9606	human|Human
10116	Rat|rat
131567	Not specified
9031	Chicken|chicken
9825	Pig
9913	Cattle
10028	Hamster
10029	Chinese hamster
8930	Pigeon
7955	Zebrafish
9598	Chimpanzee
9615	"dog, domestic"
9986	Rabbit
5811	T. gondii
'''

class RelationshipDumper(AbstractItemDumper):
    qCategories = '''
    SELECT c._category_key, c.name, st.name as stype, ot.name as otype
    FROM MGI_Relationship_Category c, ACC_MGItype st, ACC_MGIType ot
    WHERE c._mgitype_key_1 = st._mgitype_key
    AND c._mgitype_key_2 = ot._mgitype_key
    '''
    qRelationships = '''
    SELECT 
	r._relationship_key,
	r._category_key,
	r._object_key_1,
	r._object_key_2,
	rr.term as relationship,
	q.term as qualifier,
	e.abbreviation as evidencecode,
	r._refs_key
    FROM  MGI_Relationship r, VOC_Term q, VOC_Term e, VOC_Term rr
    WHERE r._category_key = %d
    AND r._relationshipterm_key = rr._term_key
    AND r._qualifier_key = q._term_key
    AND r._evidence_key = e._term_key
    ORDER BY r._relationship_key
    '''
    qProperties = '''
    SELECT r._relationship_key, t.term as property, p.value
    FROM MGI_Relationship r
      LEFT JOIN MGI_Relationship_Property p
        ON r._relationship_key = p._relationship_key
      LEFT JOIN VOC_Term t
        ON p._propertyname_key = t._term_key
    WHERE r._category_key = %d
    ORDER BY r._relationship_key, p.sequenceNum
    '''
    rTmplt = '''
<item class="%(relclass)s" id="%(id)s">
<reference name="subject" ref_id="%(subject)s" />
<reference name="object" ref_id="%(object)s" />
<attribute name="relationshipTerm" value="%(relationship)s" />
%(qualifier)s<attribute name="evidenceCode" value="%(evidencecode)s" />
%(propertystring)s<reference name="publication" ref_id="%(publication)s" />
<reference name="dataSet" ref_id="%(dataset)s" />
</item>
    '''

    def __init__(self, context, categoryKeys=None):
        AbstractItemDumper.__init__(self,context)
	if categoryKeys is None:
	    categoryKeys = self.context.QUERYPARAMS['ALL_FR_CATEGORY_KEYS']
	self.categories = {}
	cls = ''
	if len(categoryKeys):
	    cls = 'AND c._category_key in (%s)' % (",".join(map(str,categoryKeys)))
	for c in self.context.sql(self.qCategories + cls):
	    self.categories[c['_category_key']] = c

    def normalizeName(self, n, capitalizeFirst=True):
	s = n.lower().replace("-"," ").replace("_"," ").split()
	s1= capitalizeFirst and s[0].capitalize() or s[0]
        s2= ''.join(map(lambda x:x.capitalize(), s[1:]))
	return s1 + s2    

    def iterData(self, _category_key):
	qRelationships = RelationshipDumper.qRelationships % _category_key
	qProperties = RelationshipDumper.qProperties % _category_key
	i1 = self.context.sqliter(qRelationships)
	i2 = itertools.groupby(self.context.sqliter(qProperties), lambda r:r['_relationship_key'])
	for (r1,(k2,r2)) in itertools.izip(i1, i2):
	    yield r1, filter(lambda p:p['property'], list(r2))

    def dumpCategory(self, _category_key):
	cname = self.categories[_category_key]['name']
        dsid = DataSetDumper(self.context).dataSet(name="%s relationships from MGI"%cname)
	for rel, props in self.iterData(_category_key):
	    rel['id'] = self.context.makeItemId('DirectedRelationship', rel['_relationship_key'])
	    c = self.categories[rel['_category_key']]
	    rel['relclass'] = 'MGI' + self.normalizeName(cname)
	    try:
		rel['subject'] = self.context.makeItemRef(c['stype'], rel['_object_key_1'])
		rel['object'] = self.context.makeItemRef(c['otype'], rel['_object_key_2'])
	    except DumperContext.DanglingReferenceError:
	        continue
	    if rel['qualifier'] == "Not Specified":
	        rel['qualifier'] = ''
	    else:
	        rel['qualifier'] = '<attribute name="qualifier" value="%s" />\n'%rel['qualifier']
	    rel['publication'] = self.context.makeItemRef('Reference', rel['_refs_key'])
	    rel['dataset'] = dsid
	    rel['propertystring'] = ''

	    ps = []
	    for p in props:
		pn = self.normalizeName(p['property'], capitalizeFirst=False)
		ps.append('<attribute name="%s" value="%s" />\n' % (pn, self.quote(p['value'])))
	    rel['propertystring'] = ''.join(ps)

	    self.writeItem(rel, self.rTmplt)
	    # end for loop

    def mainDump(self):
	for _category_key in self.categories.keys():
	    self.context.log("%s: dumping category: %s" % \
	       (self.__class__.__name__, self.categories[_category_key]['name']))
	    self.dumpCategory(_category_key)

#

