from AbstractItemDumper import *
from DataSourceDumper import DataSetDumper

class AlleleDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
        a._allele_key, 
	a.symbol, 
	a.name, 
	m.name AS mname, 
	a._marker_key, 
	ac.accid,
        a.iswildtype, 
	a.isextinct, 
	a.ismixed,
        t1.term AS alleletype, 
        t2.term AS inheritanceMode,
        t3.term AS gltransmission,
        a._strain_key
    FROM 
        ALL_Allele a LEFT OUTER JOIN MRK_Marker m
            ON a._marker_key = m._marker_key,
        ACC_Accession ac,
        VOC_Term t1,
        VOC_Term t2,
        VOC_Term t3
    WHERE a._allele_key = ac._object_key
    AND ac._mgitype_key = %(ALLELE_TYPEKEY)d
    AND ac._logicaldb_key = %(MGI_LDBKEY)d
    AND ac.preferred = 1
    AND ac.private = 0
    AND a._allele_type_key = t1._term_key
    AND a._mode_key = t2._term_key
    AND a._transmission_key = t3._term_key

    %(LIMIT_CLAUSE)s
    '''

    ITMPLT = '''
    <item class="Allele" id="%(id)s">
      <attribute name="primaryIdentifier" value="%(accid)s" />
      <attribute name="symbol" value="%(symbol)s" />
      <reference name="organism" ref_id="%(organism)s" />
      <collection name="dataSets">%(dataSets)s</collection>
      <attribute name="name" value="%(name)s" />
      <attribute name="isWildType" value="%(iswildtype)s" />
      <attribute name="alleleType" value="%(alleletype)s" />
      <attribute name="inheritanceMode" value="%(inheritancemode)s" />
      <attribute name="glTransmission" value="%(gltransmission)s" />
      <reference name="strainOfOrigin" ref_id="%(strainid)s" />
      <collection name="mutations">%(mutations)s</collection>
      %(featureRef)s</item>
    '''

    def loadAllele2MutationMap(self):
	self.ak2mk = {}
	q  = '''
	    SELECT _allele_key, _mutation_key
	    FROM ALL_Allele_Mutation
	    '''
	for r in self.context.sql(q):
	    iref = self.context.makeItemRef('AlleleMolecularMutation',r['_mutation_key'])
	    self.ak2mk.setdefault(r['_allele_key'],[]).append(iref)

    def preDump(self):
	AlleleMutationDumper(self.context).dump()
	self.loadAllele2MutationMap()

    def processRecord(self, r):
	ak = r['_allele_key']
	r['id'] = self.context.makeItemId('Allele', ak)
	if r['mname']:
	    r['name'] = r['mname'] + "; " + r['name']
	r['strainid'] = self.context.makeItemRef('Strain', r['_strain_key'])
	self.quoteFields(r, ['symbol','name'])
	mk = r['_marker_key']
	if mk is None:
	    r['featureRef'] = ''
	else:
	    mref = self.context.makeItemRef('Marker', mk)
	    r['featureRef'] = '<reference name="feature" ref_id="%s" />' % mref
	r['iswildtype'] = r['iswildtype'] == 1 and "true" or "false"
	r['organism'] = self.context.makeItemRef('Organism', 1) # mouse
        dsid = DataSetDumper(self.context).dataSet(name="Mouse Allele Catalog from MGI")
	r['dataSets'] = '<reference ref_id="%s"/>'%dsid
	r['mutations'] = ''.join(['<reference ref_id="%s" />'%x for x in self.ak2mk.get(ak,[])])
	return r

    def postDump(self):
        self.writeCount += AlleleSynonymDumper(self.context).dump(fname="Synonym.xml")

class AlleleMutationDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT t._term_key, t.term
    FROM VOC_Term t
    WHERE t._vocab_key = %(ALLELE_MUTATION_VKEY)d
    ORDER BY t.term
    '''
    ITMPLT = '''
    <item class="AlleleMolecularMutation" id="%(id)s" >
	<attribute name="name" value="%(term)s" />
	</item>
    '''
    def processRecord(self, r):
        r['id'] = self.context.makeGlobalKey('AlleleMolecularMutation', r['_term_key'])
        return r


class AlleleSynonymDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT l._allele_key, l.label, l.labeltype
    FROM ALL_Label l
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Synonym" id="%(id)s">
      <attribute name="value" value="%(value)s" />
      <reference name="subject" ref_id="%(subject)s" />
      </item>
    '''
    def processRecord(self, r):
	r['id'] = self.context.makeItemId('Synonym')
	r['value'] = self.quote(r['label'])
	r['subject'] = self.context.makeItemRef('Allele', r['_allele_key'])
	return r

