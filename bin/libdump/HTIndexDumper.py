
from .AbstractItemDumper import *
from collections import defaultdict 
from .OboParser import OboParser

class HTIndexDumper(AbstractItemDumper):
    QTMPLT = '''
        SELECT
            e._experiment_key,
            e.name,
            e.description,
            e.release_date,
            e.lastupdate_date,
            e.evaluated_date,
            es.term as evaluationstate,
            cs.term as curationstate,
            st.term as studytype,
            et.term as experimenttype,
            s.term  as source
        FROM
            GXD_HTExperiment e,
            VOC_Term es,
            VOC_Term cs,
            VOC_Term st,
            VOC_Term et,
            VOC_Term s
        WHERE
                e._evaluationstate_key = es._term_key
            AND e._curationstate_key = cs._term_key
            AND e._studytype_key = st._term_key
            AND e._experimenttype_key = et._term_key
            AND e._source_key = s._term_key
            AND cs.term = 'Done'
    '''
    ITMPLT = '''
        <item class="GXDHTExperiment" id="%(id)s">
          <attribute name="name" value="%(name)s" />
          <attribute name="description" value="%(description)s" />
          <attribute name="studyType" value="%(studytype)s" />
          <attribute name="experimentType" value="%(experimenttype)s" />
          <attribute name="source" value="%(source)s" />
          <collection name="publications">%(pubrefs)s</collection>
          %(notes)s
          </item>
    '''
    REFTMPLT = '''
        <item class="Publication" id="%(id)s">
          <attribute name="pubMedId" value="%(pubMedId)s" />
          </item>
    '''
    def preDump (self):
        self.eksWritten = set()
        self.loadIds()
        self.loadPmids()
        self.loadPmids2Refkeys()
        self.makePubStubs()
        self.loadNotes()

    # Loads GEO and ArrayExpress experiment IDs
    def loadIds (self):
        self.ek2ids = {}
        q = '''
        SELECT a.accid, a._object_key, a._logicaldb_key
        FROM ACC_Accession a
        WHERE a._mgitype_key = %(HTEXPT_TYPEKEY)s
        ''' % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            ek = r['_object_key']
            self.ek2ids.setdefault(ek, []).append(r)

    # Loads PMIDs associated with HT Experiments that are 'Done'.
    def loadPmids (self):
        self.pmids = set()
        self.ek2pmids = {}
        q = '''
        SELECT
            e._experiment_key,
            mp.value as pmid
        FROM
            GXD_HTExperiment e,
            MGI_Property mp,
            VOC_term t
        WHERE
                e._experiment_key = mp._object_key
            AND e._curationstate_key = %(CURATIONSTATE_DONE_KEY)s
            AND mp._propertytype_key = %(HTEXPT_PROPERTYTYPE_KEY)s
            AND mp._propertyterm_key = t._term_key
            AND t.term = 'PubMed ID'
        ''' % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            ek = r['_experiment_key']
            self.ek2pmids.setdefault(ek, []).append(r['pmid'])
            self.pmids.add(r['pmid'])

    # Loads PMID-to-refs_key for Pubs in MGI.
    def loadPmids2Refkeys (self) :
        self.pmid2rk = {}
        q = '''
        SELECT accid, _object_key
        FROM ACC_Accession
        WHERE _mgitype_key = 1
        AND _logicaldb_key = 29
        '''
        for r in self.context.sql(q):
            self.pmid2rk[r['accid']] = r['_object_key']

    # HT Experiments have no direct reference associations. They only have PMID property values.
    # Some of thes PMIDs are associated with MGI references and some are not. Those that are will be
    # dumped by the PublicationDumper, and here we must create refs to those records (using th _refs_key).
    # For those that are not, we need to (1) create a stub publication object and (2) reference that.
    def makePubStubs (self):
        q = '''
        SELECT max(_refs_key) as maxkey
        FROM BIB_Refs
        '''
        maxkey = list(self.context.sql(q))[0]['maxkey']

        # Test each pubmed id associated with HT Experiments.
        # Create a Publication stub object for each one that is NOT already in MGI.
        for pmid in self.pmids:
            if pmid in self.pmid2rk:
                continue
            maxkey += 1
            self.pmid2rk[pmid] = maxkey
            r = {
              "id" : self.context.makeItemId('Reference', maxkey),
              "pubMedId" : pmid
            }
            self.writeItem(r, self.REFTMPLT)

    def loadNotes (self):
        self.ek2notes = {}
        q = '''
        SELECT n._object_key, c.note
        FROM MGI_Note n, MGI_NoteChunk c
        WHERE c._note_key = n._note_key
        AND n._notetype_key = %(HTEXPT_NOTETYPE_KEY)d
        ''' % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            ek = r['_object_key']
            self.ek2notes[ek] = '<attribute name="notes" value="%s" />' % r['note']

    def processRecord (self, r):
        ek = r['_experiment_key']
        self.eksWritten.add(ek)
        r['id'] = self.context.makeItemId('HTExperiment', ek)
        r['name'] = self.quote(r['name'])
        r['description'] = self.quote(r['description'])
        r['notes'] = self.ek2notes.get(ek, '')

        pubrefs = ''
        for pmid in self.ek2pmids.get(ek, []):
            rk = self.pmid2rk[pmid]
            ref = self.context.makeItemRef('Reference', rk)
            pubrefs += '<reference ref_id="%s" />' % ref
        r['pubrefs'] = pubrefs

        return r

    def postDump (self) :
        HTVariableDumper(self.context, self).dump()
        HTSampleDumper(self.context, self).dump()

class HTVariableDumper (AbstractItemDumper) :
    QTMPLT = '''
        SELECT
            t._term_key,
            e._experiment_key,
            t.term
        FROM
            GXD_HTExperiment e,
            GXD_HTExperimentVariable ev, 
            VOC_Term t
        WHERE
                e._experiment_key = ev._experiment_key
            AND ev._term_key = t._term_key
        '''
    ITMPLT = '''
        <item class="GXDHTVariable" id="%(id)s">
          <attribute name="name" value="%(term)s" />
          <reference name="experiment" ref_id="%(experiment)s" />
          </item>
    '''
    def processRecord (self, r) :
        ek = r['_experiment_key']
        if ek not in self.parentDumper.eksWritten:
            return None
        r['id'] = self.context.makeItemId('HTVariable', r['_term_key'])
        r['experiment'] = self.context.makeItemRef('HTExperiment', ek)
        return r


class HTSampleDumper (AbstractItemDumper) :
    QTMPLT = '''
        SELECT
            hts._sample_key
            ,hts._experiment_key
            ,hts.name
            ,hts.age
            ,hts.agemin
            ,hts.agemax
            ,r.term as relevance
            ,o.commonname as organism
            ,s.term as sex
            ,a.term as emapaterm
            ,a._term_key as _emapa_key
            ,t.stage
            ,g._genotype_key
        FROM
            gxd_htsample hts
            ,VOC_Term r
            ,MGI_Organism o
            ,VOC_Term s
            ,VOC_Term a
            ,GXD_TheilerStage t
            ,GXD_Genotype g
        WHERE
            hts._experiment_key in (
                SELECT _experiment_key
                FROM GXD_HTExperiment
                )
            AND hts._relevance_key = r._term_key
            AND hts._organism_key = o._organism_key
            AND hts._sex_key = s._term_key
            AND hts._emapa_key = a._term_key
            AND hts._stage_key = t._stage_key
            AND hts._genotype_key = g._genotype_key

    '''
    ITMPLT = '''
        <item class="GXDHTSample" id="%(id)s">
          <attribute name="name" value="%(name)s" />
          <attribute name="sex" value="%(sex)s" />
          <attribute name="stage" value="%(stage)s" />
          <attribute name="age" value="%(age)s" />
          <attribute name="ageMin" value="%(agemin)s" />
          <attribute name="ageMax" value="%(agemax)s" />
          <reference name="experiment" ref_id="%(experiment)s" />
          <reference name="genotype" ref_id="%(genotype)s" />
          <reference name="structure" ref_id="%(emapa)s" />
          %(notes)s
          </item>
    '''
    def preDump (self) :
        self.loadNotes()

    def loadNotes (self):
        self.sk2notes = {}
        q = '''
        SELECT n._object_key, c.note
        FROM MGI_Note n, MGI_NoteChunk c
        WHERE c._note_key = n._note_key
        AND n._notetype_key = %(HTSAMPLE_NOTETYPE_KEY)d
        ''' % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            sk = r['_object_key']
            self.sk2notes[sk] = '<attribute name="notes" value="%s" />' % r['note']

    def processRecord (self, r) :
        ek = r['_experiment_key']
        if not ek in self.parentDumper.eksWritten:
            return None
        r['id'] = self.context.makeItemId('HTSample', r['_sample_key'])
        r['experiment'] = self.context.makeItemRef('HTExperiment', ek)
        r['genotype'] = self.context.makeItemRef('Genotype', r['_genotype_key'])
        r['emapa'] = self.context.makeItemRef('EMAPATerm', r['_emapa_key'])
        r['notes'] = self.sk2notes.get(r['_sample_key'], '')
        return r

    def postDump (self) :
        pass
