from AbstractItemDumper import *

class AlleleDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT a._allele_key, a.symbol, a.name, a._marker_key, ac.accid,
	a.iswildtype, a.isextinct, a.ismixed,
	t.term AS alleletype, a._strain_key
    FROM 
	ALL_Allele a LEFT OUTER JOIN MRK_Marker m
	    ON a._marker_key = m._marker_key,
	ACC_Accession ac,
	VOC_Term t
    WHERE a._allele_key = ac._object_key
    AND ac._mgitype_key = %(ALLELE_TYPEKEY)d
    AND ac._logicaldb_key = %(MGI_LDBKEY)d
    AND ac.preferred = 1
    AND ac.private = 0
    AND a._allele_type_key = t._term_key

    %(LIMIT_CLAUSE)s
    '''

    ITMPLT = '''
    <item class="Allele" id="%(id)s">
      <attribute name="primaryIdentifier" value="%(accid)s" />
      <attribute name="symbol" value="%(symbol)s" />
      <attribute name="name" value="%(name)s" />
      <attribute name="isWildType" value="%(iswildtype)s" />
      <attribute name="alleleType" value="%(alleletype)s" />
      <reference name="strainOfOrigin" ref_id="%(strainid)s" />
      %(featureRef)s</item>
    '''

    def processRecord(self, r):
	r['id'] = self.context.makeItemId('Allele', r['_allele_key'])
	r['strainid'] = self.context.makeItemRef('Strain', r['_strain_key'])
	self.quoteFields(r, ['symbol','name'])
	mk = r['_marker_key']
	if mk is None:
	    r['featureRef'] = ''
	else:
	    mref = self.context.makeItemRef('Marker', mk)
	    r['featureRef'] = '<reference name="feature" ref_id="%s" />' % mref
	return r

    def postDump(self):
        self.writeCount += AlleleSynonymDumper(self.context).dump(fname="Synonym.xml")

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

