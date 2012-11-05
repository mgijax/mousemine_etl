
from AbstractItemDumper import *
from DumperContext import DumperContext
from OboParser import OboParser
from DataSourceDumper import DataSetDumper
import os

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
	    ve._refs_key
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
	  </item>
	''']

    def __init__(self, ctx):
        AbstractItemDumper.__init__(self,ctx)
	self.atk2classes = { # FIXME. Hardcoded annottype data
	# annottype_key : ( annotated obj type, voc term class, voc evidence class, evidence code class, species )
	# Mouse marker-GO annotations
	1000 : ('Marker', 'GOTerm','GOAnnotation','GOEvidence','GOEvidenceCode','Mouse'),
	# Mouse genotype-MP annotations
	1002 : ('Genotype', 'MPTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse'),
	# Mouse genotype-OMIM annotations
	1005 : ('Genotype', 'DiseaseTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse'),
	# Human gene-OMIM annotations
	1006 : ('Marker', 'DiseaseTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Human'),
	}
	self.ANNOTTYPEKEYS = self.atk2classes.keys()
	self.ANNOTTYPEKEYS_S = COMMA.join(map(lambda x:str(x),self.ANNOTTYPEKEYS))
        self.context.QUERYPARAMS['ANNOTTYPEKEYS'] = self.ANNOTTYPEKEYS_S

    def preDump(self):
	#
	# Keep track of ontology terms referenced by the annotations.
	# (Since ontologies are loaded seperately, here we need
	# to just write out stubs for the OntologyTerm items.)
	#
	self.termsToWrite = set() # the terms we need to create stubs for
	self.assignedkeys = {}	# identifier -> key (eg: 'GO:123456' -> '10013_1001')
	self.omimRemap = {}	# OMIM id -> MeSH id
	if hasattr(self.context, 'moshfile'):
	    mfile = os.path.abspath(os.path.join(os.getcwd(),self.context.moshfile))
	    self.loadOmimMappings(mfile)
	self.writeDataSets()

    def writeDataSets(self):
	dsd = DataSetDumper(self.context)
	self.atk2dsid = {}
	for atk, atinfo in self.atk2classes.items():
	    dsname = '%s to %s %s Annotations from MGI' % (atinfo[1],atinfo[5],atinfo[0])
	    self.atk2dsid[atk] = dsd.dataSet(name=dsname)
    #
    # Reads the MOSH ontology file to build a mapping from
    # OMIM id -> MeSH id (for OMIM terms that are merged)
    #
    def loadOmimMappings(self, file):
	def stanzaProc( stype, slines ):
	    id = ""
	    for tag,val in slines:
		if tag=="id" and val.startswith("OMIM"):
		    self.omimRemap[val]=val
		    return
	        if tag=="id" and val.startswith("MESH"):
		    id = val
		elif tag=="alt_id" and val.startswith("OMIM"):
		    self.omimRemap[val] = id
	OboParser(stanzaProc).parseFile(file)

    def processRecord(self, r, iQuery):
	atk = r['_annottype_key']
	tname, oclass, aclass, aeclass, aecclass, aspecies = self.atk2classes[atk]
	if iQuery == 0:
	    # OntologyAnnotation
	    r['id'] = self.context.makeItemId('OntologyAnnotation', r['_annot_key'])
	    r['subject'] = self.context.makeItemRef(tname, r['_object_key'])
	    r['qualifier'] = r['qualifier'] and ('<attribute name="qualifier" value="%(qualifier)s"/>' % r) or ''
	    r['class'] = aclass
	    r['dataSets'] = '<reference ref_id="%s"/>'%self.atk2dsid[atk]

	    identifier = r['identifier']
	    tk = r['_term_key']
	    if oclass == 'DiseaseTerm':
		# remap an annotation to an OMIM id to the appropriate MOSH id
		omimid = "OMIM:"+identifier
	        identifier = self.omimRemap.get(omimid,None)
		if identifier is None:
		    self.context.log('Annotation skipped. Ontology term id not found: %s\n%s\n' % (omimid,str(r)))
		    return None
		r['identifier'] = identifier
		tk = self.assignedkeys.setdefault(identifier, r['_term_key'])
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
		return r
	else:
	    # OntologyAnnotationEvidence
	    r['id'] = self.context.makeItemId('OntologyAnnotationEvidence')
	    r['class'] = aeclass
	    r['code'] = self.context.makeItemRef('OntologyAnnotationEvidenceCode', r['_evidenceterm_key'])
	    r['annotation'] = self.context.makeItemRef('OntologyAnnotation', r['_annot_key'])
	    r['inferredfrom'] = r['inferredfrom'] and ('<attribute name="inferredFrom" value="%(inferredfrom)s"/>'%r) or ''
	    r['publications'] = '<reference ref_id="%s"/>' % \
	        self.context.makeItemRef('Reference', r['_refs_key'])
	    return r

    def postDump(self):
	# Write out stub items for OntologyTerms.
	tmplt = '''<item class="%(class)s" id="%(id)s"> <attribute name="identifier" value="%(identifier)s" /> </item>\n'''
        for t in self.termsToWrite:
	    r = { 'class':t[0], 'id':t[1], 'identifier':t[2] }
	    self.writeItem(r, tmplt)
