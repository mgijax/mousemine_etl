
from .AbstractItemDumper import *
from .DumperContext import DumperContext
from .OboParser import OboParser
from .DataSourceDumper import DataSetDumper
import os
import re

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
          # Mouse allele-derived MP annotation
          1028 : ('Derived MPTerm to Mouse Allele Annotations from MGI',
                  'Allele', 'MPTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse', True),
          # Mouse allele-derived DO annotation
          1029 : ('Derived DiseaseTerm to Mouse Allele Annotations from MGI',
                  'Allele', 'DOTerm','OntologyAnnotation','OntologyAnnotationEvidence','OntologyAnnotationEvidenceCode','Mouse', True),
        }
        self.ANNOTTYPEKEYS = list(self.atk2classes.keys())
        self.ANNOTTYPEKEYS_S = COMMA.join([str(x) for x in self.ANNOTTYPEKEYS])
        self.context.QUERYPARAMS['ANNOTTYPEKEYS'] = self.ANNOTTYPEKEYS_S
        self.ANNOTTYPEKEYS_PROPS = [k for k in self.ANNOTTYPEKEYS if self.atk2classes[k][7]] # keys where loadProperties is true

    def preDump(self):
        #
        # Keep track of ontology terms referenced by the annotations.
        # (Since ontologies are loaded seperately, here we need
        # to just write out stubs for the OntologyTerm items.)
        #
        self.termsToWrite = set() # the terms we need to create stubs for
        self.assignedkeys = {}  # identifier -> key (eg: 'GO:123456' -> '10013_1001')
        self.loadAnnotsToExclude()
        self.loadEvidenceProperties()
        self.writeDataSets()

    # Hack because there are cases of GO annotations to withdrawn markers. We need to excluded these
    # annots because otherwise, we get a DanglingReferenceError when we try to create the marker object ref.
    def loadAnnotsToExclude(self):
        self.annotsToExclude = set()
        q = '''
            SELECT va._annot_key 
            FROM voc_annot va, mrk_marker mm
            WHERE va._object_key = mm._marker_key
            AND va._annottype_key = 1000
            AND mm._marker_status_key = 2
            '''
        for r in self.context.sql(q):
            self.annotsToExclude.add(r['_annot_key'])

        self.context.log("Excluding %d annotations." % len(self.annotsToExclude) )

    def writeDataSets(self):
        self.dsd = DataSetDumper(self.context)
        self.atk2dsid = {}
        for atk, atinfo in list(self.atk2classes.items()):
            dsname = atinfo[0]
            self.atk2dsid[atk] = self.dsd.dataSet(name=dsname)

    #
    # Loads specific kinds of annotation evidence property records.
    # Creates an index from annotation key lists of property values.
    #
    # For MP-genotype annotations, looks for sex-specificity notes.
    # For derived annotations, looks for the key(s) of the underlying base annotations.
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
        ''' % (','.join([str(x) for x in self.ANNOTTYPEKEYS_PROPS]))
        for r in self.context.sql(q):
            atk = r['_annottype_key']
            if atk == 1002:
                v = r['value'].upper()
                if v in "MF":
                    ek = r['_annotevidence_key']
                    self.ek2props[ek] = 'specific_to(%s)' % (v == 'M' and 'male' or 'female')
            elif atk in [1015, 1023, 1028, 1029]:
                if r['term'] == "_SourceAnnot_key":
                    self.ek2props.setdefault(r['_annotevidence_key'],[]).append(int(r['value']))

    def processRecord(self, r, iQuery):
        atk = r['_annottype_key']
        dsname, tname, oclass, aclass, aeclass, aecclass, aspecies, ahasprops = self.atk2classes[atk]
        if iQuery == 0:
            # OntologyAnnotation
            if r['_annot_key'] in self.annotsToExclude:
                self.context.log("Excluding annotation: " + str(r))
                return None
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
            if r['_annot_key'] in self.annotsToExclude:
                return None
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
            elif r['_annottype_key'] in [1015,1023,1028,1029]:
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

