from AbstractItemDumper import *
from DataSourceDumper import DataSetDumper
from NoteUtils import iterNotes
import re

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
      <attribute name="isRecombinase" value="%(isRecombinase)s" />
      %(description)s %(drivenBy)s %(inducedWith)s 
      %(featureRef)s
      </item>
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

    def _loadNotes(self, _notetype_key, parser=None):
	ak2notes = {}
        for n in iterNotes(_notetype_key=_notetype_key):
	    n['note'] = parser(n['note']) if parser else n['note']
	    k = n['_object_key']
	    if k in ak2notes:
	        ak2notes[k] += ' ' + n['note']
	    else:
	        ak2notes[k] = n['note']
	return ak2notes

    def loadNotes(self):
	i_re = re.compile(r'([Ii]nduc(ed|ibl[ey]) +(by|with) +|-induc(ed|ible)|\. *$)')
	def parseInducibleNote(n):
	    return self.quote(i_re.sub('',n))
        self.ak2generalnotes = self._loadNotes( 1020, self.quote )
	self.ak2molecularnotes = self._loadNotes( 1021, self.quote )
	self.ak2drivernotes = self._loadNotes( 1034, self.quote )
	self.ak2induciblenotes = self._loadNotes( 1032, parseInducibleNote )

	# for now, we're concatenating molecular notes to the end of the general notes
	for (ak, mn) in self.ak2molecularnotes.iteritems():
	    gn = self.ak2generalnotes.get(ak, '')
	    self.ak2generalnotes[ak] = gn + ' ' + mn

    def preDump(self):
	AlleleMutationDumper(self.context).dump()
	self.loadAllele2MutationMap()
	self.loadNotes()

    def processRecord(self, r):
	ak = r['_allele_key']
	r['id'] = self.context.makeItemId('Allele', ak)
	if r['mname']:
	    r['name'] = r['mname'] + "; " + r['name']
	r['strainid'] = self.context.makeItemRef('Strain', r['_strain_key'])
	mk = r['_marker_key']
	if mk is None:
	    r['featureRef'] = ''
	else:
	    mref = self.context.makeItemRef('Marker', mk)
	    r['featureRef'] = '<reference name="feature" ref_id="%s" />' % mref
	r['iswildtype'] = "true" if (r['iswildtype'] == 1) else "false"
	r['organism'] = self.context.makeItemRef('Organism', 1) # mouse
        dsid = DataSetDumper(self.context).dataSet(name="Mouse Allele Catalog from MGI")
	r['dataSets'] = '<reference ref_id="%s"/>'%dsid
	r['mutations'] = ''.join(['<reference ref_id="%s" />'%x for x in self.ak2mk.get(ak,[])])

	r['isRecombinase'] = "true" if self.ak2drivernotes.has_key(ak) else "false"

	def setNote(r, ak, dct, aname):
	    n = dct.get(ak,None)
	    r[aname] = '<attribute name="%s" value="%s" />' % (aname,n) if n else ''

	setNote(r, ak, self.ak2generalnotes, 'description')
	setNote(r, ak, self.ak2drivernotes, 'drivenBy')
	setNote(r, ak, self.ak2induciblenotes, 'inducedWith')

	r['symbol'] = self.quote(r['symbol'])
	r['name']   = self.quote(r['name'])

	return r

    def postDump(self):
        self.writeCount += AlleleSynonymDumper(self.context).dump(fname="Synonym.xml")
	self.ak2generalnotes = None
	self.ak2mk = None

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

