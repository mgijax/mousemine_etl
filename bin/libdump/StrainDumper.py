from AbstractItemDumper import *

class StrainDumper(AbstractItemDumper):
    QTMPLT='''
    SELECT a.accid, s._strain_key, s.strain AS name, t.term AS straintype, s.standard
    FROM
      PRB_Strain s JOIN VOC_Term t
      ON s._straintype_key = t._term_key
    LEFT OUTER JOIN ACC_Accession a
      ON s._strain_key = a._object_key
      AND a._mgitype_key = %(STRAIN_TYPEKEY)s
      AND a._logicaldb_key = 1
      AND a.preferred = 1
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Strain" id="%(id)s" >
      <reference name="organism" ref_id="%(organism)s" />
      <attribute name="primaryIdentifier" value="%(accid)s" />
      <attribute name="name" value="%(name)s" />
      <collection name="publications">%(publications)s</collection>
      <attribute name="attributeString" value="%(attributeString)s" />
      <collection name="attributes">%(attributes)s</collection>
      </item>
    '''
    def loadStrainPubs(self):
        self.sk2pk = {}
	q='''
	SELECT ra._refs_key, ra._object_key as "_strain_key"
	FROM MGI_Reference_Assoc ra
	WHERE ra._refassoctype_key in (%s)
	''' % ','.join([ str(x) for x in self.context.QUERYPARAMS['STRAIN_REFASSOCTYPE_KEYS']])
	for r in self.context.sql(q):
	    self.sk2pk.setdefault( r['_strain_key'], []).append(self.context.makeItemRef('Reference', r['_refs_key']))

    def loadStrainAttrs(self):
        self.sk2attrs = {}
	self.sk2typestring = {}
	q = '''
	SELECT va._object_key as _strain_key, va._term_key, vt.term
	FROM VOC_Annot va, VOC_Term vt
	WHERE va._annottype_key = %(STRAIN_ATTRIBUTE_AKEY)s
	AND va._term_key = vt._term_key
	''' % self.context.QUERYPARAMS
	for r in self.context.sql(q):
	    iref = self.context.makeItemRef('StrainAttribute', r['_term_key'])
	    self.sk2attrs.setdefault(r['_strain_key'],[]).append(iref)
	    self.sk2typestring.setdefault(r['_strain_key'],[]).append(r['term'])

    def preDump(self):
	StrainAttributeDumper(self.context).dump()
        self.loadStrainPubs()
	self.loadStrainAttrs()

    def getOrganismRefForStrain(self, s):
	taxon = self.context.QUERYPARAMS['STRAIN_ORGANISM'].get(s, 10090)
	org   = self.context.QUERYPARAMS['ORGANISMS'][taxon]
	ref   = self.context.makeItemRef('Organism', org[0])
        return ref

    def processRecord(self, r):
	sk = r['_strain_key']
	r['id'] = self.context.makeItemId('Strain', sk)
	r['organism'] = self.getOrganismRefForStrain(r['name'])
	r['name'] = self.quote(r['name'])
	r['attributeString'] = ', '.join(self.sk2typestring.get(sk,[]))
	if r['attributeString'] == '':
	    r['attributeString'] = 'Not specified'
	r['publications'] = ''.join(['<reference ref_id="%s"/>'%x for x in self.sk2pk.get(sk,[])])
	r['attributes'] = ''.join(['<reference ref_id="%s" />'%x for x in self.sk2attrs.get(sk,[])])
        return r

class StrainAttributeDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT t._term_key, t.term
    FROM VOC_Term t
    WHERE t._vocab_key = %(STRAIN_ATTRIBUTE_VKEY)d
    ORDER BY t.term
    '''
    ITMPLT = '''
    <item class="StrainAttribute" id="%(id)s" >
	<attribute name="name" value="%(term)s" />
	</item>
    '''
    def processRecord(self, r):
        r['id'] = self.context.makeGlobalKey('StrainAttribute', r['_term_key'])
        return r
