
from AbstractItemDumper import *
from DumperContext import DumperContext
from OboParser import OboParser
from DataSourceDumper import DataSetDumper
import os
import re
from DerivedAnnotationHelper import DerivedAnnotationHelper

class AnnotationDumper(AbstractItemDumper):
    QTMPLT = [
	#
	# Get data for each annotation.
	#
        '''
	SELECT 
	    va._annottype_key, 
	    va._annot_key, 
	    vat._vocab_key, 
	    va._term_key, 
	    qt.term AS qualifier,
	    aa.accid AS identifier, 
	    va._object_key
	FROM 
	    VOC_Annot va, 
	    VOC_Term vt, 
	    VOC_Term qt, 
	    VOC_AnnotType vat, 
	    VOC_Vocab vv, 
	    ACC_Accession aa
	WHERE va._annottype_key in (%(ANNOTTYPEKEYS)s)
	AND va._qualifier_key = qt._term_key
	AND va._annottype_key = vat._annottype_key
	AND va._term_key = vt._term_key
	AND vt._term_key = aa._object_key
	AND aa._mgitype_key = %(TERM_TYPEKEY)d
	AND aa.preferred = 1
	AND aa.private = 0
	AND vt._vocab_key = vv._vocab_key
	AND aa._logicaldb_key = vv._logicaldb_key
	%(LIMIT_CLAUSE)s
	''',
	#
	# Evidence code vocabulary terms.
	# WARNING: This query will return dup terms if > 1 annotation type
	# uses the same evidence vocab. (Necessary because we need the annot
	# type key in the results.)
	#
	'''
	SELECT 
	    v._term_key, 
	    v.abbreviation AS code, 
	    v.term, 
	    vat._annottype_key
	FROM 
	    VOC_Term v, 
	    VOC_AnnotType vat
	WHERE vat._evidencevocab_key = v._vocab_key
	AND vat._annottype_key in (%(ANNOTTYPEKEYS)s)
	%(LIMIT_CLAUSE)s
	''',
	#
	# Get annotation evidence info, e.g., evidence code, reference, etc.
	#
	'''
	SELECT 
	    ve._annotevidence_key, 
	    ve._annot_key, 
	    va._annottype_key,
	    ve._evidenceterm_key,
	    ve.inferredfrom, 
	    ve._refs_key,
	    ve.creation_date
	FROM 
	    VOC_Evidence ve, 
	    VOC_Annot va 
	WHERE ve._annot_key = va._annot_key
	AND va._annottype_key in (%(ANNOTTYPEKEYS)s)
	%(LIMIT_CLAUSE)s
	''']

    ITMPLT = [
	# OntologyAnnotation
	# The actual class name is a parameter (to accommodate GO-annotations,
	# which are their own subclasses in the standard model).
	# Do not explicitly set evidence collection (let reverse refs take care of that)
	# References collection is not yet implemented.
	# DataSets collection not yet implemented
	'''
	<item class="%(class)s" id="%(id)s">
	  <reference name="ontologyTerm" ref_id="%(ontologyterm)s"/>
	  <reference name="subject" ref_id="%(subject)s"/>
	  %(qualifier)s
	  <collection name="dataSets">%(dataSets)s</collection>
	  </item>
	''',
	#
	# OntologyAnnotationEvidenceCode
	#
	'''
	<item class="%(class)s" id="%(id)s">
	  <attribute name="code" value="%(code)s"/>
          <attribute name="name" value="%(name)s"/>
	  </item>
	''',
	#
	# OntologyAnnotationEvidence
	#
	'''
	<item class="%(class)s" id="%(id)s">
	  <reference name="annotation" ref_id="%(annotation)s"/>
	  %(inferredfrom)s
	  <reference name="code" ref_id="%(code)s"/>
	  <collection name="publications">%(publications)s</collection>
	  %(annotationExtension)s
	  <collection name="baseAnnotations">%(baseAnnotations)s</collection>
          <collection name="comments">%(comments)s</collection>
	  %(annotationDate)s
	  </item>
	''',
	]

    def __init__(self, ctx):
        AbstractItemDumper.__init__(self,ctx)
	self.atk2classes = { 
	  # FIXME. Hardcoded annotation-type data. This really needs refactoring!
	  # annottype_key : 
	  #    ( annotated obj type, 
	  #      voc term class, 
	  #      voc evidence class, 
	  #      evidence code class, 
	  #      species, 
	  #      loadProperties )
	  #
	  # Mouse marker-GO annotations
	  1000 : ('GOTerm to Mouse Feature Annotations from MGI',
	          'Marker', 'GOTerm','GOAnnotation','GOEvidence','GOEvidenceCode','Mouse', False),
	  # Mouse genotype-MP annotations
	  1002 : ('MPTerm to Mouse Genotype Annotations from MGI',
	          'Genotype', 'MPTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse', True),
	  # Mouse genotype-DO annotations
	  1020 : ('DiseaseTerm to Mouse Genotype Annotations from MGI',
	          'Genotype', 'DOTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse', False),
	  # Human gene-DO annotations
	  1022 : ('DiseaseTerm to Human Feature Annotations from MGI',
	          'Marker', 'DOTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Human', False),
	  # Mouse allele-DO annotations
	  1021 : ('DiseaseTerm to Mouse Allele Annotations from MGI',
	          'Allele', 'DOTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse', False),
	  # Mouse marker-derived MP annotation
	  1015 : ('MPTerm to Mouse Feature Annotations from MGI',
	          'Marker', 'MPTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse', True),
	  # Mouse marker-derived DO annotation
	  1023 : ('DiseaseTerm to Mouse Feature Annotations from MGI',
	          'Marker', 'DOTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse', True),
	}
	self.ANNOTTYPEKEYS = self.atk2classes.keys()
	self.ANNOTTYPEKEYS_S = COMMA.join(map(lambda x:str(x),self.ANNOTTYPEKEYS))
        self.context.QUERYPARAMS['ANNOTTYPEKEYS'] = self.ANNOTTYPEKEYS_S
	self.ANNOTTYPEKEYS_PROPS = filter( lambda k:self.atk2classes[k][7], self.ANNOTTYPEKEYS ) # keys where loadProperties is true

    def preDump(self):
	#
	# Keep track of ontology terms referenced by the annotations.
	# (Since ontologies are loaded seperately, here we need
	# to just write out stubs for the OntologyTerm items.)
	#
	self.termsToWrite = set() # the terms we need to create stubs for
	self.assignedkeys = {}	# identifier -> key (eg: 'GO:123456' -> '10013_1001')
	self.loadEvidenceProperties()
	self.writeDataSets()

    def writeDataSets(self):
	self.dsd = DataSetDumper(self.context)
	self.atk2dsid = {}
	for atk, atinfo in self.atk2classes.items():
	    dsname = atinfo[0]
	    self.atk2dsid[atk] = self.dsd.dataSet(name=dsname)

    #
    # Loads the evidence property records for the specified annotation types.
    # Creates an index from annotation key to the annotation extension (a string).
    #
    # In MGI, annotation extensions (VOC_Evidence_Property records) are attached to 
    # evidence records, but in Intermine, they are  associated with
    # Annotation objects. 
    #
    # RIGHT NOW ONLY WORKS FOR MP ANNOTATIONS (annotationtype key=1002)!!! 
    # GO annotations (key=1000) also have property records
    # in MGI, but the translation into GAF annotation extensions is complex,
    # See TR11112. We are not loading these yet.
    # 
    # No other annotations in MGI have property records at this time.
    #
    def loadEvidenceProperties(self):
	self.ek2props = {}
        q = '''
            select 
	        a._annottype_key, 
		a._annot_key, 
		p._annotevidence_key, 
		p.stanza, 
		p.sequencenum, 
		t.term, 
		p.value
            from 
	        voc_evidence_property p, 
		voc_term t, 
		voc_evidence e, 
		voc_annot a
            where p._propertyterm_key = t._term_key
            and p._annotevidence_key = e._annotevidence_key
            and e._annot_key = a._annot_key
	    and a._annottype_key in (%s)
            order by p._annotevidence_key, p.stanza, p.sequencenum
        ''' % (','.join(map(lambda x:str(x), self.ANNOTTYPEKEYS_PROPS)))
        for r in self.context.sql(q):
	    atk = r['_annottype_key']
	    if atk == 1002:
		v = r['value'].upper()
		if v in "MF":
		    ek = r['_annotevidence_key']
		    self.ek2props[ek] = 'specific_to(%s)' % (v == 'M' and 'male' or 'female')
	    elif atk == 1015 or atk == 1023:
		if r['term'] == "_SourceAnnot_key":
		    self.ek2props.setdefault(r['_annotevidence_key'],[]).append(int(r['value']))

    def processRecord(self, r, iQuery):
	atk = r['_annottype_key']
	dsname, tname, oclass, aclass, aeclass, aecclass, aspecies, ahasprops = self.atk2classes[atk]
	if iQuery == 0:
	    # OntologyAnnotation
	    r['id'] = self.context.makeItemId('OntologyAnnotation', r['_annot_key'])
	    r['subject'] = self.context.makeItemRef(tname, r['_object_key'])
	    r['qualifier'] = r['qualifier'] and ('<attribute name="qualifier" value="%(qualifier)s"/>' % r) or ''
	    r['class'] = aclass
	    r['dataSets'] = '<reference ref_id="%s"/>'%self.atk2dsid[atk]

	    identifier = r['identifier']

	    tk = r['_term_key']
	    # make the reference without checking (because this dumper will
	    # create them later).
	    otermkey = self.context.makeGlobalKey('Vocabulary Term', tk)
	    r['ontologyterm'] = otermkey
	    self.termsToWrite.add( (oclass, otermkey, identifier) )
	    return r
	elif iQuery == 1:
	    # OntologyEvidenceCode
	    try:
		r['id'] = self.context.makeItemId('OntologyAnnotationEvidenceCode', r['_term_key'])
	    except DumperContext.DuplicateIdError:
	        return
	    else:
		r['class'] = aecclass
		r['name'] = r['term']
		return r
	else:
	    # OntologyAnnotationEvidence
	    r['id'] = self.context.makeItemId('OntologyAnnotationEvidence', r['_annotevidence_key'])
	    r['class'] = aeclass
	    r['code'] = self.context.makeItemRef('OntologyAnnotationEvidenceCode', r['_evidenceterm_key'])
	    r['annotation'] = self.context.makeItemRef('OntologyAnnotation', r['_annot_key'])
	    r['inferredfrom'] = r['inferredfrom'] and ('<attribute name="withText" value="%(inferredfrom)s"/>'%r) or ''
	    r['publications'] = '<reference ref_id="%s"/>' % \
	        self.context.makeItemRef('Reference', r['_refs_key'])

	    r['baseAnnotations'] = ''
	    r['annotationExtension'] = ''
	    if r['_annottype_key'] == 1002:
		p = self.ek2props.get(r['_annotevidence_key'])
		p = p and '<attribute name="annotationExtension" value="%s" />'%p or ''
		r['annotationExtension'] = p
	    elif r['_annottype_key'] in [1015,1023]:
		ps = self.ek2props.get(r['_annotevidence_key'],[])
		refs = [ self.context.makeItemRef('OntologyAnnotation', k) for k in ps ]
		refs2 = [ '<reference ref_id="%s"/>'%ref for ref in refs ]
		r['baseAnnotations'] = ''.join(refs2)

            r['comments'] = ''.join(self.context.annotationComments.get(r['_annotevidence_key'],[]))
	    r['annotationDate'] = \
	      '<attribute name="annotationDate" value="%s"/>' % r['creation_date'].strftime('%Y-%m-%d')
	    return r

    def postDump(self):
	# Write out stub items for OntologyTerms.
	tmplt = '''<item class="%(class)s" id="%(id)s"> <attribute name="identifier" value="%(identifier)s" /> </item>\n'''
        for t in self.termsToWrite:
	    r = { 'class':t[0], 'id':t[1], 'identifier':t[2] }
	    self.writeItem(r, tmplt)

	# switch output files
	self.context.openOutput("DerivedAnnotations.xml")

	# need one more evidence code 
	c = {}
	c['id'] = self.context.makeItemId('OntologyAnnotationEvidenceCode')
	c['class']= "OntologyAnnotationEvidenceCode"
	c['code'] = "DOA"
	c['name'] = "derived from other annotations"
	self.writeItem(c, self.ITMPLT[1])

	# compute and write out derived annotations
	dsref = self.dsd.dataSet(name="DiseaseTerm to Mouse Allele Annotations from MGI")
	helper = DerivedAnnotationHelper(self.context)

	def writeDerivedAnnot( type, k, vk, tk, arks ):
	    # Args:
	    #	type	"Marker" or "Allele"
	    #	k	its MGI database key
	    #	vk	vocabulary key
	    #	tk	term key
	    #	arks	object containing: a set of annotation keys (annots),
	    #		a set of reference keys (refs), and (if applicable) an
	    #		existing annotation key
	    # NOTE: helper data is gotten from the MGI independently. Possible that some of the
	    # objects might have been skipped by the dumper code prior to this. 
	    # Therefore handle dangling reference errors by skipping the record..
	    # 
	    try:
		#
		r = {}
		if arks['existing']:
		    r['id'] = self.context.makeItemRef('OntologyAnnotation', arks['existing'])
		else:
		    r['id'] = self.context.makeItemId('OntologyAnnotation') # start auto-assigning.
		    r['class'] = "OntologyAnnotation"
		    r['subject'] = self.context.makeItemRef(type, k)
		    r['ontologyterm'] = self.context.makeItemRef('Vocabulary Term', tk)
		    r['qualifier'] = ''
		    r['dataSets'] = '<reference ref_id="%s"/>' % dsref
		#
		s = {}
		s['id'] = self.context.makeItemId('OntologyAnnotationEvidence') # start auto-assigning
		s['class'] = 'OntologyAnnotationEvidence'
		s['annotation'] = r['id']
		s['inferredfrom'] = ''
		s['code'] = c['id']
		#
		ars = []
		for ak in arks['annots']:
		    try:
			ar = self.context.makeItemRef("OntologyAnnotation", ak)
			ars.append('<reference ref_id="%s" />'%ar)
		    except DumperContext.DanglingReferenceError:
		        pass
		s['baseAnnotations'] = ''.join(ars)
		#
		rrs = []
		for rk in arks['refs']:
		    try:
			rr = self.context.makeItemRef("Reference", rk)
			rrs.append('<reference ref_id="%s" />'%rr)
		    except DumperContext.DanglingReferenceError:
		        pass
		s['publications'] = ''.join(rrs)
		s['annotationExtension'] = ''

                s['comments'] = ''
		s['annotationDate'] = ''

		#
	    except DumperContext.DanglingReferenceError:
	        pass
	    else:
		if not arks['existing']:
		    self.writeItem(r, self.ITMPLT[0])
		self.writeItem(s, self.ITMPLT[2])
	    
	#for (mk, vk, tk, arks) in helper.iterAnnots("Marker"):
	#    writeDerivedAnnot("Marker", mk, vk, tk, arks)
	for (ak, vk, tk, arks) in helper.iterAnnots("Allele"):
	    writeDerivedAnnot("Allele", ak, vk, tk, arks)

