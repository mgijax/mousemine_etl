from .AbstractItemDumper import *
from .DataSourceDumper import DataSetDumper
from .NoteUtils import iterNotes
import re

class AlleleDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
        a._allele_key, 
        a.symbol, 
        a.name, 
        m.name AS mname, 
        m._marker_status_key,
        m._marker_key, 
        ac.accid,
        a.iswildtype, 
        a.isextinct, 
        a.ismixed,
        t1.term AS alleletype, 
        t2.term AS inheritanceMode,
        t3.term AS gltransmission,
        t4.term AS projectcollection,
        a._strain_key
    FROM 
        ALL_Allele a LEFT OUTER JOIN MRK_Marker m
            ON a._marker_key = m._marker_key,
        ACC_Accession ac,
        VOC_Term t1,
        VOC_Term t2,
        VOC_Term t3,
        VOC_Term t4
    WHERE a._allele_key = ac._object_key
    AND ac._mgitype_key = %(ALLELE_TYPEKEY)d
    AND ac._logicaldb_key = %(MGI_LDBKEY)d
    AND ac.preferred = 1
    AND ac.private = 0
    AND a._allele_type_key = t1._term_key
    AND a._mode_key = t2._term_key
    AND a._transmission_key = t3._term_key
    AND a._collection_key = t4._term_key

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
      <collection name="alleleAttributes">%(alleleAttributes)s</collection>
      <attribute name="inheritanceMode" value="%(inheritancemode)s" />
      <attribute name="glTransmission" value="%(gltransmission)s" />
      <collection name="publications">%(publications)s</collection>
      <collection name="publications2">%(publications2)s</collection>
      %(earliestPublication)s
      <reference name="strainOfOrigin" ref_id="%(strainid)s" />
      <collection name="mutations">%(mutations)s</collection>
      <collection name="carriedBy">%(carriedBy)s</collection>
      <attribute name="isRecombinase" value="%(isRecombinase)s" />
      %(attributeString)s %(projectcollection)s %(description)s %(molecularNote)s %(inducedWith)s 
      %(featureRef)s
      </item>
    '''

    def loadAllele2StrainMap(self):
        self.ak2sk = {}
        q = '''
        SELECT pm._strain_key, pm._allele_key
        FROM PRB_Strain_Marker pm
        WHERE pm._allele_key is not null
        '''
        for r in self.context.sql(q):
            iref = self.context.makeItemRef('Strain', r['_strain_key'])
            self.ak2sk.setdefault(r['_allele_key'],[]).append(iref)

    def loadAllele2MutationMap(self):
        self.ak2mk = {}
        q  = '''
            SELECT _allele_key, _mutation_key
            FROM ALL_Allele_Mutation
            '''
        for r in self.context.sql(q):
            iref = self.context.makeItemRef('AlleleMolecularMutation',r['_mutation_key'])
            self.ak2mk.setdefault(r['_allele_key'],[]).append(iref)

    def loadAllele2AttributeMap(self):
        self.ak2atrs = {}
        self.ak2atrss= {}
        q = '''
            SELECT va._object_key AS _allele_key, va._term_key AS _attribute_key, vt.term
            FROM VOC_Annot va, VOC_Term vt
            WHERE va._annottype_key = %(ALLELE_ATTRIBUTE_AKEY)d
            AND va._term_key = vt._term_key
            ORDER BY _allele_key, term
            ''' % self.context.QUERYPARAMS
        for r in self.context.sql(q):
            iref = self.context.makeItemRef('AlleleAttribute',r['_attribute_key'])
            self.ak2atrs.setdefault(r['_allele_key'],[]).append(iref)
            self.ak2atrss.setdefault(r['_allele_key'],[]).append(r['term'])
            

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
        self.ak2induciblenotes = self._loadNotes( 1032, parseInducibleNote )

    def loadAllelePublications(self):
        # map from allele key to set of publication refs
        self.pub_refs = {}
        # map from allele key to list of (type, pubref) pairs
        self.pub_refs2 = {}
        q = '''
        SELECT ra._refs_key, ra._object_key, rat.assoctype
        FROM MGI_Reference_Assoc ra, MGI_RefAssocType rat 
        WHERE ra._mgitype_key = 11
        AND ra._refassoctype_key = rat._refassoctype_key
        '''
        for r in self.context.sql(q):
            okey = r['_object_key']
            if okey not in self.pub_refs:
              self.pub_refs[okey] = set()
              self.pub_refs2[okey] = []
            pr = self.context.makeItemRef('Reference', r['_refs_key'])
            self.pub_refs[okey].add(pr)
            self.pub_refs2[okey].append((r['assoctype'], pr))

    def loadEarliestPublications(self):
       self.earliest_publications = {}
       q = '''
           select distinct aa._allele_key AS _allele_key, br._refs_key AS _refs_key, br.year
           from MGI_Reference_Assoc ra, BIB_Refs br, ALL_Allele aa
           where ra._refs_key = br._refs_key
           and ra._object_key = aa._allele_key
           and ra._mgitype_key = 11
           order by aa._allele_key, br.year, br._refs_key
           '''
       current_allele_key = 0;
       found_citeable = False
       for r in self.context.sql(q):
           if current_allele_key != r['_allele_key']:
               found_citeable = False
               # use the first publciation even if it is unciteable
               self.earliest_publications[r['_allele_key']] = self.context.makeItemRef('Reference', r['_refs_key'])
               if self.context.isPubCiteable(r['_refs_key']):
                   found_citeable = True 
           else:
               # if there are multiple publications use the fist citeable one 
               if not found_citeable:
                   if self.context.isPubCiteable(r['_refs_key']):
                       self.earliest_publications[r['_allele_key']] = self.context.makeItemRef('Reference', r['_refs_key'])
                       found_citeable = True 
           current_allele_key = r['_allele_key']

    def preDump(self):
        AlleleMutationDumper(self.context).dump()
        AlleleAttributeDumper(self.context).dump()
        self.apd = AllelePublicationDumper(self.context)
        self.apd.dump(fname="AllelePublications.xml")
        self.context.openOutput(fname="Allele.xml")
        self.loadAllele2StrainMap()
        self.loadAllele2MutationMap()
        self.loadAllele2AttributeMap()
        self.loadNotes()
        # self.loadAllelePublications()
        self.loadEarliestPublications()


    def processRecord(self, r):
        if r['_marker_status_key'] in [2,3]:
            return None
        ak = r['_allele_key']
        r['id'] = self.context.makeItemId('Allele', ak)
        if r['mname'] and r['mname'] != r['name']:
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
        r['publications'] = ''.join(['<reference ref_id="%s"/>'%x for x in self.apd.ak2pubrefs.get(ak,[])])
        r['publications2'] = ''.join(['<reference ref_id="%s"/>'%x for x in self.apd.ak2apk.get(ak,[])])
        r['carriedBy'] = ''.join(['<reference ref_id="%s"/>'%x for x in self.ak2sk.get(ak,[])])

        ep = self.earliest_publications.get(ak)
        if  ep is None:
            r['earliestPublication'] = ''
        else:
            r['earliestPublication'] = '<reference name="earliestPublication" ref_id="%s" />' % ep


        def setNote(r, ak, dct, aname):
            n = dct.get(ak,None)
            r[aname] = '<attribute name="%s" value="%s" />' % (aname,n) if n else ''

        setNote(r, ak, self.ak2generalnotes, 'description')
        setNote(r, ak, self.ak2molecularnotes, 'molecularNote')
        setNote(r, ak, self.ak2induciblenotes, 'inducedWith')

        r['symbol'] = self.quote(r['symbol'])
        r['name']   = self.quote(r['name'])

        r['alleleAttributes'] = ''.join(['<reference ref_id="%s" />'%x for x in self.ak2atrs.get(ak,[])])
        atrs = self.ak2atrss.get(ak,None)
        r['attributeString'] = '' if atrs is None else \
          '<attribute name="attributeString" value="%s" />\n' % ', '.join(self.ak2atrss.get(ak,[])) 
        r['isRecombinase'] = "true" if "Recombinase" in r['attributeString'] else "false"

        if r['projectcollection'] == "Not Specified":
            r['projectcollection'] = ''
        else:
            r['projectcollection'] = \
              '<attribute name="projectCollection" value="%s" />\n' % r['projectcollection']
        return r


    def postDump(self):
        self.writeCount += AlleleSynonymDumper(self.context).dump(fname="Synonym.xml")
        self.ak2generalnotes = None
        self.ak2mk = None

class AlleleAttributeDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT t._term_key, t.term
    FROM VOC_Term t
    WHERE t._vocab_key = %(ALLELE_ATTRIBUTE_VKEY)d
    ORDER BY t.term
    '''
    ITMPLT = '''
    <item class="AlleleAttribute" id="%(id)s" >
        <attribute name="name" value="%(term)s" />
        </item>
    '''
    def preDump(self):
        # There is a term where the name is 'Not applicable'. Don't want to
        # to mask that.
        self.suppressNA = False

    def processRecord(self, r):
        r['id'] = self.context.makeGlobalKey('AlleleAttribute', r['_term_key'])
        return r

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

class AllelePublicationDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT ra._assoc_key, ra._refs_key, ra._object_key, rat.assoctype
    FROM MGI_Reference_Assoc ra, MGI_RefAssocType rat, ACC_Accession aa
    WHERE ra._mgitype_key = 11
    AND ra._refassoctype_key = rat._refassoctype_key
    AND ra._refs_key = aa._object_key
    AND aa._mgitype_key = 1
    AND aa._logicaldb_key = 1
    AND aa.prefixpart = 'J:'
    '''
    ITMPLT = '''
    <item class="AllelePublication" id="%(id)s" >
        <attribute name="type" value="%(type)s"/>
        <reference name="publication" ref_id="%(publication)s" />
        </item>
    '''
    def preDump (self):
        self.ak2apk = {} # _allele_key -> [ AllelePublication references ]
        self.ak2pubrefs = {} # _allele_key -> [ Publication references ]

    def processRecord(self, r):
        r['id'] = self.context.makeGlobalKey('AllelePublication', r['_assoc_key'])
        r['type'] = r['assoctype']
        r['publication'] = self.context.makeItemRef('Reference', r['_refs_key'])
        ak = r['_object_key']
        self.ak2apk.setdefault(ak, []).append(r['id'])
        self.ak2pubrefs.setdefault(ak, set()).add(r['publication'])
        return r


class AlleleSynonymDumper(AbstractItemDumper):
    QTMPLT = '''
        SELECT l._allele_key, l.label
        FROM ALL_Label l, ALL_Allele a, MRK_Marker m
        WHERE l.labeltypename = 'synonym'
        AND l._allele_key = a._allele_key
        AND a._marker_key = m._marker_key
        AND m._marker_status_key = 1

        UNION

        SELECT l._allele_key, l.label
        FROM ALL_Label l, ALL_Allele a
        WHERE l.labeltypename = 'synonym'
        AND l._allele_key = a._allele_key
        AND a._marker_key is null

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

