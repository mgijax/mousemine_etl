#
# HTIndexDumper.py
#
# Dumps curated metadata for high-throughput expression experiments and samples.
#
# Creates HTExperiment and HTSample objects
#
# Creates some Publication stubs.
#
# HTExperiments may reference Publications created by this dumper or by the
# Publication dumper.
#
# For correct operation, the Publication dumper MUST RUN FIRST!!
#

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
        <item class="HTExperiment" id="%(id)s">
          %(experimentId)s
          %(seriesId)s
          <attribute name="name" value="%(name)s" />
          <attribute name="description" value="%(description)s" />
          <attribute name="studyType" value="%(studytype)s" />
          <attribute name="experimentType" value="%(experimenttype)s" />
          <collection name="variables">%(variables)s</collection>
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
    HTVARTMPLT = '''
        <item class="HTVariable" id="%(id)s">
          <attribute name="name" value="%(term)s" />
          </item>
    '''
    def preDump (self):
        self.eksWritten = set()
        self.loadIds()
        self.loadPmids()
        self.loadPmids2Refkeys()
        self.makePubStubs()
        self.loadNotes()
        self.writeVariableTerms()
        self.loadVariables()

    # Loads GEO and ArrayExpress IDs for all HT experiments. In MGI, an experiment ID 
    def loadIds (self):
        self.ek2ids = {}
        q = '''
        SELECT a.accid, a._object_key, a._logicaldb_key, a.preferred
        FROM ACC_Accession a
        WHERE a._mgitype_key = %(HTEXPT_TYPEKEY)s
        AND a._object_key in (
            SELECT _experiment_key
            FROM GXD_HTExperiment
            WHERE _curationstate_key = %(CURATIONSTATE_DONE_KEY)s
        )
        ''' % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            ek = r['_object_key']
            rids = self.ek2ids.setdefault(ek, {})
            if r['preferred'] == 1:
                rids['experimentId'] = '<attribute name="experimentId" value="%s" />' % r['accid']
            else:
                rids['seriesId'] = '<attribute name="seriesId" value="%s" />' % r['accid']

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

    # Loads PMID-to-refs_key for all Pubs in MGI.
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

    # In MGI, HT Experiments have no direct reference associations; they only have PMID property values.
    # Many of these PMIDs already exist in MGI and can be converted to a publication reference; the Publication
    # dumper will create the objects.
    # Many PMIDs do not exist in MGI, so here we have to create objects (stubs) for them.
    def makePubStubs (self):
        # assign keys that
        q = 'SELECT max(_refs_key) as maxkey FROM BIB_Refs'
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

    # Loads all notes associated with HTExperiments
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
            self.ek2notes[ek] = '<attribute name="notes" value="%s" />' % self.quote(r['note'])

    # Writes the HT Variable vocabulary (_vocab_key = 122)
    def writeVariableTerms (self) :
        q = '''select _term_key, term from VOC_Term where _vocab_key = %(HT_VARIABLES_VKEY)s''' % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            r['id'] = self.context.makeItemId('HTVariable', r['_term_key'])
            self.writeItem(r, self.HTVARTMPLT)

    # Loads variable keys associated with HT Experiments
    def loadVariables (self) :
        self.ek2vars = {}
        q = '''
        SELECT
            e._experiment_key,
            ev._term_key
        FROM
            GXD_HTExperiment e,
            GXD_HTExperimentVariable ev
        WHERE
                e._experiment_key = ev._experiment_key
        '''  % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            ek = r['_experiment_key']
            self.ek2vars.setdefault(ek, []).append(r['_term_key'])

    #
    def processRecord (self, r):
        ek = r['_experiment_key']
        self.eksWritten.add(ek)
        r['id'] = self.context.makeItemId('HTExperiment', ek)
        rids = self.ek2ids.get(ek, {})
        r['experimentId'] = rids.get('experimentId', '')
        r['seriesId'] = rids.get('seriesId', '')
        r['name'] = self.quote(r['name'])
        r['description'] = self.quote(r['description'])
        r['notes'] = self.ek2notes.get(ek, '')
        vrefs = ''
        for vk in self.ek2vars.get(ek,[]):
            vrefs += '<reference ref_id="%s" />' % self.context.makeItemRef('HTVariable', vk)
        r['variables'] = vrefs
        #r['variables'] = "|".join(self.ek2vars.get(ek,[]))

        rids = self.ek2ids[ek]

        # create a Publication reference for each PMID.
        pubrefs = ''
        for pmid in self.ek2pmids.get(ek, []):
            rk = self.pmid2rk[pmid]
            ref = self.context.makeItemRef('Reference', rk)
            pubrefs += '<reference ref_id="%s" />' % ref
        r['pubrefs'] = pubrefs

        return r

    #
    def postDump (self) :
        #HTVariableDumper(self.context, self).dump()
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
        <item class="HTSample" id="%(id)s">
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
            self.sk2notes[sk] = '<attribute name="notes" value="%s" />' % self.quote(r['note'])

    def processRecord (self, r) :
        ek = r['_experiment_key']
        if not ek in self.parentDumper.eksWritten:
            return None
        r['id'] = self.context.makeItemId('HTSample', r['_sample_key'])
        r['name'] = self.quote(r['name'])
        r['age'] = self.quote(r['age'])
        r['experiment'] = self.context.makeItemRef('HTExperiment', ek)
        r['genotype'] = self.context.makeItemRef('Genotype', r['_genotype_key'])
        r['emapa'] = self.context.makeItemRef('EMAPATerm', r['_emapa_key'])
        r['notes'] = self.sk2notes.get(r['_sample_key'], '')
        return r

    def postDump (self) :
        pass
