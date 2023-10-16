from .AbstractItemDumper import *
from .DataSourceDumper import DataSetDumper
from .DumperContext import DumperContext

class FeatureDumper(AbstractItemDumper):
    ITMPLT = '''
    <item class="SOTerm" id="%(id)s">
        <attribute name="identifier" value="%(soid)s" />
        </item>
    '''
    def mainDump(self):
        md=MouseFeatureDumper(self.context)
        nd=HumanFeatureDumper(self.context)
        od=OtherSpeciesFeatureDumper(self.context)
        self.context.soIds = set(["SO:0000340","SO:0005858"]) # need Chromosome and SyntenicRegion as well
        self.writeCount += md.dump(**self.dumpArgs)
        self.writeCount += nd.dump(**self.dumpArgs)
        self.writeCount += od.dump(**self.dumpArgs)

    def postDump(self):
        soids = list(self.context.soIds)
        soids.sort()
        self.context.log(str(soids))
        for s in soids:
            id = self.context.makeGlobalKey('SOTerm',int(s.split(':')[1]))
            self.writeItem( {'id':id, 'soid':s}, self.ITMPLT)
        
class AbstractFeatureDumper(AbstractItemDumper):
    ITMPLT = '''
    <item class="%(featureClass)s" id="%(id)s" >
      <attribute name="primaryIdentifier" value="%(primaryidentifier)s" />
      <attribute name="mgiType" value="%(mcvType)s" />
      %(soterm)s
      <attribute name="symbol" value="%(symbol)s" />
      <attribute name="name" value="%(name)s" />
      %(description)s
      %(specificityNote)s
      %(ncbiGeneNumber)s
      %(partnerRef)s
      <reference name="organism" ref_id="%(organismid)s" />
      <reference name="chromosome" ref_id="%(chromosomeid)s" />
      %(locationRef)s
      <collection name="publications">%(publications)s</collection>
      %(earliestPublication)s
      <collection name="dataSets">%(dataSets)s</collection>
      </item>
    '''

    def preloadDescriptions(self):
        # Preload all description notes for mouse markers. Two separate notes from MGI are concatenated
        # into a single note in MouseMine, which goes into the description field. A given gene may have
        # neither, either, or both. Here's an example:
        self.mk2description = {}
        #
        # First, the gene function overview note
        q = self.constructQuery('''
            select n._object_key as _marker_key, n.note
            from MGI_Note n, MRK_Marker m
            where n._object_key = m._marker_key
            and n._notetype_key = 1014
            and m._organism_key = 1
            order by n._object_key
            ''')
        for r in self.context.sql(q):
            mk = r['_marker_key']
            note = 'FUNCTION: ' + r['note'].replace("<hr><B>Summary from NCBI RefSeq</B><BR><BR>","").replace("<hr>","")
            self.mk2description[mk] = self.mk2description.get(mk,'') + note 
        #
        # Second, the phenotype overview note. 
        q = '''
            select n._marker_key, n.note
            from MRK_Notes n, MRK_Marker m
            where n._marker_key = m._marker_key
            and m._organism_key = 1
            order by n._marker_key
            '''
        for r in self.context.sql(q):
            mk = r['_marker_key']
            note = 'PHENOTYPE: ' + r['note'] + (' [provided by MGI curators]')
            note0 = self.mk2description.get(mk,'')
            note0 += ' <br> ' if note0 else '' # add line break if there's a function note
            self.mk2description[mk] = note0 + note 

    def preloadMarkerReferenceAssociations(self):
        self.mk2refs = {}
        q = self.constructQuery('''
            SELECT _marker_key, _refs_key
            FROM MRK_Reference
            ''')
        for r in self.context.sql(q):
            id = self.context.makeGlobalKey('Reference',r['_refs_key'])
            if id in self.context.idsWritten:
                self.mk2refs.setdefault(r['_marker_key'],[]).append('<reference ref_id="%s"/>'%id)

    def preloadEntrezIds(self):
        self.mk2entrez = {}
        q = self.constructQuery('''
          SELECT accid, _object_key
          FROM ACC_Accession
          WHERE _logicaldb_key = %(ENTREZ_LDBKEY)d
          AND _mgitype_key = %(MARKER_TYPEKEY)d
          ''')
        for r in self.context.sql(q):
            self.mk2entrez[r['_object_key']] = \
                '<attribute name="ncbiGeneNumber" value="%s" />' % r['accid']

    def preloadStrainSpecificityNotes(self):
        self.mk2specificityNote = {}
        q = self.constructQuery('''
            SELECT m._marker_key, n.note
            FROM MGI_Note n, MRK_Marker m
            WHERE n._object_key = m._marker_key
            AND n._notetype_key = %(STRAIN_SPECIFIC_NOTETYPE_KEY)d
            AND m._marker_status_key = %(OFFICIAL_STATUS)d 
            ''')
        for r in self.context.sql(q):
            self.mk2specificityNote[r['_marker_key']] = r['note']

    def preloadEarliestPubs(self):
        self.earliest_publications = {}
        q = '''
            select distinct mr._marker_key AS _marker_key, mr._refs_key AS _refs_key, br.year, mr.jnum
            from mrk_reference mr, bib_refs br
            where mr._refs_key = br._refs_key
            order by _marker_key, br.year, mr.jnum
            '''
        current_marker_key = 0
        found_citeable = False
        for r in self.context.sql(q):
            if current_marker_key != r['_marker_key']:
                found_citeable = False                
                # use the first publication even if it is unciteable
                self.earliest_publications[r['_marker_key']] = self.context.makeItemRef('Reference', r['_refs_key'])
                if self.context.isPubCiteable(r['_refs_key']):
                    found_citeable = True
            else:
                # if there are multiple publciations use the first one that is citeable
                if not found_citeable:
                    if self.context.isPubCiteable(r['_refs_key']):
                        self.earliest_publications[r['_marker_key']] = self.context.makeItemRef('Reference', r['_refs_key'])
                        found_citeable = True
            current_marker_key = r['_marker_key']         


    def preloadPARtners(self):
        self.mk2PARtner = {}
        self.partnerForwardReferers = []
        q = '''
            SELECT _object_key_1, _object_key_2
            FROM MGI_Relationship
            WHERE _category_key = 1012
        '''
        for r in self.context.sql(q):
            self.mk2PARtner[r['_object_key_1']] = r['_object_key_2']

    def preDump(self):
        self.preloadEntrezIds()
        self.preloadMarkerReferenceAssociations()
        self.preloadDescriptions()
        self.preloadStrainSpecificityNotes()
        self.preloadEarliestPubs()
        self.preloadPARtners()


    def getDescription(self, r):
        n = self.mk2description.get(r['_marker_key'], '')
        if n:
            n = '<attribute name="description" value="%s" />' % self.quote(n)
        return n

    def getSpecificityNote(self, r):
        snote = self.mk2specificityNote.get(r['_marker_key'], '')
        if snote:
            snote = '<attribute name="specificity" value="%s" />' % self.quote(snote)
        return snote

    def getClass(self, r):
        return MCV2ClassName[self.getMcvType(r)]

    def getMcvType(self, r):
        pass    #override me

    def getLocationRef(self, r):
        if r['startcoordinate'] is not None:
            # have to do this without checking - this ref is created *before* 
            # the location is.
            ref = self.context.makeGlobalKey('Location', r['_marker_key'])
            return '<reference name="chromosomeLocation" ref_id="%s" />' % ref
        else:
            return ''


    def getDataSetRef(self):
        return "" # override me


    # Special processing for the ncbiGeneNumber attribute. In the Intermine core model,
    # this attr is introduced in class Gene. However some MGI features have NCBI (Entrez)
    # gene ids but are not genes (or subtypes), e.g. some "Other Genome Feature" objects
    # have Entrez ids. 
    def getNcbiGeneNumberAttribute(self,r):
        # a more robust test would be to consult the SO ontology, but this
        # happens to work, so...
        fc =  r['featureClass']
        if 'Gene' not in fc or 'Pseudo' in fc or fc == 'GeneSegment':
            return ''
        return self.mk2entrez.get(r['_marker_key'], '')

    def getPARtnerRef(self, r):
        if r['_marker_key'] in self.mk2PARtner:
            pk = self.mk2PARtner[r['_marker_key']]
            pref = self.context.makeGlobalKey('Marker', pk, None) 
            return '<reference name="partner" ref_id="%s" />' % pref
        else:
            return ''

    def processRecord(self, r):
        fclass, soId = self.getClass(r)
        if fclass is None:
            return None
        #r['id'] = self.context.makeItemId('Marker', r['_marker_key'])
        r['id'] = self.context.makeGlobalKey('Marker', r['_marker_key'], None)
        r['featureClass'] = fclass
        r['mcvType'] = self.getMcvType(r)
        r['description'] = self.getDescription(r)
        r['specificityNote'] = self.getSpecificityNote(r)
        r['ncbiGeneNumber'] = self.getNcbiGeneNumberAttribute(r)
        r['locationRef'] = self.getLocationRef(r)
        r['organismid'] = self.context.makeItemRef('Organism', r['_organism_key']) 
        r['chromosomeid'] = self.context.makeItemRef('Chromosome', r['_chromosome_key']) 
        if soId:
            self.context.soIds.add(soId)
            r['soterm'] = '<reference name="sequenceOntologyTerm" ref_id="%s"/>' % \
                self.context.makeGlobalKey('SOTerm',int(soId.split(":")[1]))
        else:
            r['soterm'] = ''
        r['publications'] = ''.join(self.mk2refs.get(r['_marker_key'],[]))
        ep = self.earliest_publications.get(r['_marker_key'])
        if ep is None:
            r['earliestPublication'] = ''
        else:
            r['earliestPublication'] = '<reference name="earliestPublication" ref_id="%s" />' % ep
        r['dataSets'] = self.getDataSetRef()
        r['symbol'] = self.quote(r['symbol'])
        r['name'] = self.quote(r['name'])
        #
        r['partnerRef'] = self.getPARtnerRef(r)
        #
        return r

class MouseFeatureDumper(AbstractFeatureDumper):
    # Changed query 1/14/2013. No need for outer join to location cache table: every mouse marker
    # has exactly one entry in mrk_location_cache.
    # The join to mrk_chromosome uses the chromosome field in the marker table, which is the genetic
    # chromosome. This is OK to leave as is.
    QTMPLT = '''
    SELECT m._organism_key, m._marker_key, m.symbol, m.name, mc._chromosome_key,
        c.term AS mcvtype, a.accid AS primaryidentifier, lc.startcoordinate
    FROM 
        MRK_Marker m
        LEFT OUTER JOIN ACC_Accession a
            ON m._marker_key = a._object_key
            AND a._mgitype_key = %(MARKER_TYPEKEY)d
            AND a._logicaldb_key = %(MGI_LDBKEY)d
            AND a.preferred = 1
            AND a.private = 0,
        MRK_Location_Cache lc,
        MRK_MCV_Cache c, 
        MRK_Chromosome mc
    WHERE m._organism_key = 1
    AND m._marker_key = lc._marker_key
    AND m._marker_status_key = (%(OFFICIAL_STATUS)d)
    AND m._marker_key = c._marker_key
    AND c.qualifier = 'D'
    AND m.chromosome = mc.chromosome
    AND m._organism_key = mc._organism_key
    %(LIMIT_CLAUSE)s
    '''

    def getMcvType(self, r):
        return r['mcvtype']

    def getDataSetRef(self):
        dsid = DataSetDumper(self.context).dataSet(name="Mouse Gene Catalog from MGI")
        return '<reference ref_id="%s"/>'%dsid

class HumanFeatureDumper(AbstractFeatureDumper):
    QTMPLT = '''
    SELECT m._organism_key, m._marker_key, m.symbol, m.name, mc._chromosome_key,
        t.name AS mgitype, a.accid AS primaryidentifier, lc.startCoordinate
    FROM 
        MRK_Marker m, 
        MRK_Location_Cache lc,
        MRK_Types t, 
        MRK_Chromosome mc, 
        ACC_Accession a
    WHERE m._organism_key = 2
    AND m._marker_key = lc._marker_key
    AND m.chromosome = mc.chromosome
    AND m._organism_key = mc._organism_key
    AND m._marker_type_key = t._marker_type_key
    AND m._marker_key = a._object_key
    AND a._mgitype_key = %(MARKER_TYPEKEY)d
    AND a._logicaldb_key = %(ENTREZ_LDBKEY)d
    AND a.preferred = 1
    AND a.private = 0
    %(LIMIT_CLAUSE)s
    '''
    def getMcvType(self, r):
        return MGIType2MCVType[r['mgitype']]

    def getDataSetRef(self):
        dsid = DataSetDumper(self.context).dataSet(
                name="Human Genes from EntrezGene",
                dataSource=self.context.dataSourceByName["Entrez Gene"] )
        return '<reference ref_id="%s"/>'%dsid

class OtherSpeciesFeatureDumper(AbstractFeatureDumper):
    QTMPLT = '''
    SELECT m._organism_key, m._marker_key, m.symbol, m.name, 
        t.name AS mgitype, a.accid AS primaryidentifier
    FROM 
        MRK_Types t, 
        MRK_Marker m LEFT OUTER JOIN 
            ACC_Accession a ON m._marker_key = a._object_key
            AND a._mgitype_key = %(MARKER_TYPEKEY)d
            AND a._logicaldb_key = %(ENTREZ_LDBKEY)d
            AND a.preferred = 1
            AND a.private = 0
    WHERE m._organism_key not in (1,2)
    AND m._marker_status_key = 1
    AND m._marker_type_key = t._marker_type_key
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="%(featureClass)s" id="%(id)s" >
      <attribute name="primaryIdentifier" value="%(primaryidentifier)s" />
      <attribute name="mgiType" value="%(mcvType)s" />
      %(soterm)s
      <attribute name="symbol" value="%(symbol)s" />
      <attribute name="name" value="%(name)s" />
      %(ncbiGeneNumber)s
      <reference name="organism" ref_id="%(organismid)s" />
      <collection name="dataSets">%(dataSets)s</collection>
      </item>
    '''
    def getMcvType(self, r):
        return MGIType2MCVType[r['mgitype']]

    def getDataSetRef(self):
        dsid = DataSetDumper(self.context).dataSet(name="Non-mouse/non-human genes from MGI")
        return '<reference ref_id="%s"/>'%dsid

    def processRecord(self, r):
        fclass, soId = self.getClass(r)
        if fclass is None:
            return None
        r['id'] = self.context.makeItemId('Marker', r['_marker_key'])
        if not r['primaryidentifier']:
            r['primaryidentifier'] = "key_" + str(r['_marker_key'])
        r['featureClass'] = fclass
        r['mcvType'] = self.getMcvType(r)
        r['ncbiGeneNumber'] = self.getNcbiGeneNumberAttribute(r)
        r['organismid'] = self.context.makeItemRef('Organism', r['_organism_key']) 
        if soId:
            self.context.soIds.add(soId)
            r['soterm'] = '<reference name="sequenceOntologyTerm" ref_id="%s"/>' % \
                self.context.makeGlobalKey('SOTerm',int(soId.split(":")[1]))
        else:
            r['soterm'] = ''
        r['dataSets'] = self.getDataSetRef()
        r['symbol'] = self.quote(r['symbol'])
        r['name'] = self.quote(r['name'])
        return r

# Map from MGI type names to equivalent MCV terms
# (used only for non-mouse features)
MGIType2MCVType = {
    'Gene'                      : 'gene',
    'DNA Segment'               : 'DNA segment',
    'Cytogenetic Marker'        : 'cytogenetic marker',
    'QTL'                       : 'QTL',
    'Pseudogene'                : 'Pseudogene',
    'BAC/YAC end'               : 'BAC/YAC end',
    'Other Genome Feature'      : 'other genome feature',
    'Complex/Cluster/Region'    : 'complex/cluster/region',
    'Transgene'                 : 'transgene',
    }

#
# This table maps an MCV type name (from the MGI type ontology) to a pair consisting of (1) the
# name of the class of object to create and (2) the SO term id to assign to it. 
# When a feature from MGI is mapped to a feature in MouseMine, it will have the named class
# and will have a sequenceOntologyTerm reference to the term with the given SO term.
#
MCV2ClassName = {
    'antisense lncRNA gene'     : ('NcRNAGene',                         'SO:0002182'),
    'BAC end'                   : ('Read',                              'SO:0000999'),
    'BAC/YAC end'               : ('Read',                              'SO:0000150'),
    'bidirectional promoter lncRNA gene': ('NcRNAGene',                 'SO:0002185'),
    'chromosomal deletion'      : ('ChromosomeStructureVariation',      'SO:1000029'),
    'chromosomal duplication'   : ('ChromosomeStructureVariation',      'SO:1000037'),
    'chromosomal inversion'     : ('ChromosomeStructureVariation',      'SO:1000030'),
    'chromosomal translocation' : ('ChromosomeStructureVariation',      'SO:1000044'),
    'chromosomal transposition' : ('ChromosomeStructureVariation',      'SO:0000453'),
    'complex/cluster/region'    : ('ComplexClusterRegion',              'SO:0001411'),
    'CpG island'                : ('OtherGenomeFeature',                'SO:0000307'),
    'CTCF binding site'         : ('RegulatoryRegion',                  'SO:0001974'),
    'cytogenetic marker'        : ('ChromosomeStructureVariation',      'SO:1000183'),
    'DNA segment'               : ('DNASegment',                        'SO:0000110'),
    'endogenous retroviral region':('OtherGenomeFeature',               'SO:0000903'),
    'enhancer'                  : ('RegulatoryRegion',                  'SO:0000165'),
    'gene'                      : ('Gene',                              'SO:0000704'),
    'gene segment'              : ('GeneSegment',                       'SO:3000000'),
    'heritable phenotypic marker' : ('HeritablePhenotypicMarker',       'SO:0001500'),
    'histone modification'      : ('RegulatoryRegion',                  'SO:0001700'),
    'imprinting control region' : ('RegulatoryRegion',                  'SO:0002191'),
    'insertion'                 : ('Insertion',                         'SO:0000667'),
    'insulator'                 : ('RegulatoryRegion',                  'SO:0000627'),
    'insulator binding site'    : ('RegulatoryRegion',                  'SO:0001460'),
    'intronic regulatory region': ('RegulatoryRegion',                  'SO:0001492'),
    'lincRNA gene'              : ('NcRNAGene',                         'SO:0001641'),
    'locus control region'      : ('RegulatoryRegion',                  'SO:0000037'),
    'lncRNA gene'               : ('NcRNAGene',                         'SO:0002127'),
    'minisatellite'             : ('OtherGenomeFeature',                'SO:0000643'),
    'miRNA gene'                : ('NcRNAGene',                         'SO:0001265'),
    'mutation defined region'   : ('OtherGenomeFeature',                'SO:0000110'),
    'non-coding RNA gene'       : ('NcRNAGene',                         'SO:0001263'),
    'open chromatin region'     : ('OpenChromatinRegion',               'SO:0001747'),
    'origin of replication'     : ('OriginOfReplication',               'SO:0000296'),
    'other feature type'        : ('OtherGenomeFeature',                'SO:0000110'),
    'other genome feature'      : ('OtherGenomeFeature',                'SO:0000110'),
    'PAC end'                   : ('Read',                              'SO:0001480'),
    'polymorphic pseudogene'    : ('PolymorphicPseudogene',             'SO:0001841'),
    'promoter'                  : ('RegulatoryRegion',                  'SO:0000167'),
    'promoter flanking region'  : ('RegulatoryRegion',                  'SO:0001952'),
    'protein coding gene'       : ('ProteinCodingGene',                 'SO:0001217'),
    'pseudogene'                : ('Pseudogene',                        'SO:0000336'),
    'pseudogenic gene segment'  : ('PseudogenicGeneSegment',            'SO:0001741'),
    'pseudogenic region'        : ('PseudogenicRegion',                 'SO:0000462'),
    'QTL'                       : ('QTL',                               'SO:0000771'),
    'reciprocal chromosomal translocation' : \
                                  ('ChromosomeStructureVariation',      'SO:1000048'),
    'response element'          : ('RegulatoryRegion',                  'SO:0002205'),
    'retrotransposon'           : ('OtherGenomeFeature',                'SO:0000180'),
    'ribozyme gene'             : ('NcRNAGene',                         'SO:0002181'),
    'RNase MRP RNA gene'        : ('NcRNAGene',                         'SO:0001640'),
    'RNase P RNA gene'          : ('NcRNAGene',                         'SO:0001639'),
    'Robertsonian fusion'       : ('ChromosomeStructureVariation',      'SO:1000043'),
    'rRNA gene'                 : ('NcRNAGene',                         'SO:0001637'),
    'scRNA gene'                : ('NcRNAGene',                         'SO:0001266'),
    'sense intronic lncRNA gene': ('NcRNAGene',                         'SO:0002184'),
    'sense overlapping lncRNA gene': ('NcRNAGene',                      'SO:0002183'),
    'silencer'                  : ('RegulatoryRegion',                  'SO:0000625'),
    'snoRNA gene'               : ('NcRNAGene',                         'SO:0001267'),
    'snRNA gene'                : ('NcRNAGene',                         'SO:0001268'),
    'splice enhancer'           : ('RegulatoryRegion',                  'SO:0000344'),
    'SRP RNA gene'              : ('NcRNAGene',                         'SO:0001269'),
    'telomerase RNA gene'       : ('NcRNAGene',                         'SO:0001643'),
    'telomere'                  : ('OtherGenomeFeature',                'SO:0000624'),
    'transcription factor binding site' : ('RegulatoryRegion',          'SO:0000235'),
    'transcriptional cis regulatory region' : ('TranscriptionalCisRegulatoryRegion', 'SO:0001055'),
    'transgene'                 : ('Transgene',                         'SO:0000902'),
    'tRNA gene'                 : ('NcRNAGene',                         'SO:0001272'),
    'TSS cluster'                : ('OtherGenomeFeature',               'SO:0001915'),
    'unclassified cytogenetic marker'   : \
                                  ('ChromosomeStructureVariation',      'SO:1000183'),
    'unclassified gene'         : ('Gene',                              'SO:0000704'),
    'unclassified non-coding RNA gene'  : \
                                  ('NcRNAGene',                         'SO:0001263'),
    'unclassified other genome feature' : \
                                  ('OtherGenomeFeature',                'SO:0000110'),
    'YAC end'                   : ('Read',                              'SO:0001498'),

    }

