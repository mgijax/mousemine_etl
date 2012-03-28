from AbstractItemDumper import *
from DataSourceDumper import DataSetDumper

class FeatureDumper(AbstractItemDumper):
    ITMPLT = '''
    <item class="SOTerm" id="%(id)s">
	<attribute name="identifier" value="%(soid)s" />
	</item>
    '''
    def mainDump(self):
	md=MouseFeatureDumper(self.context)
	nd=NonMouseFeatureDumper(self.context)
	self.context.soIds = set(["SO:0000340","SO:0005858"]) # need Chromosome and SyntenicRegion as well
	self.writeCount += md.dump(**self.dumpArgs)
	self.writeCount +=nd.dump(**self.dumpArgs)

    def postDump(self):
        soids = list(self.context.soIds)
	soids.sort()
	for s in soids:
	    id = self.context.makeItemId('SOTerm',int(s.split(':')[1]))
	    self.writeItem( {'id':id, 'soid':s}, self.ITMPLT)
        
class AbstractFeatureDumper(AbstractItemDumper):
    ITMPLT = '''
    <item class="%(featureClass)s" id="%(id)s" >
      <attribute name="primaryIdentifier" value="%(primaryidentifier)s" />
      %(soterm)s
      <attribute name="symbol" value="%(symbol)s" />
      <attribute name="name" value="%(name)s" />
      %(ncbiGeneNumber)s
      <reference name="organism" ref_id="%(organismid)s" />
      <reference name="chromosome" ref_id="%(chromosomeid)s" />
      %(locationRef)s
      <collection name="publications">%(publications)s</collection>
      <collection name="dataSets">%(dataSets)s</collection>
      </item>
    '''

    def preDump(self):
	# preload all the NCBI EntrezGene IDs into a dict {markerkey->entrezid}
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

	# preload all Marker/Reference associations
	self.mk2refs = {}
	q = self.constructQuery('''
	    SELECT _marker_key, _refs_key
	    FROM MRK_Reference
	    ''')
	for r in self.context.sql(q):
	    id = self.context.makeGlobalKey('Reference',r['_refs_key'])
	    if id in self.context.idsWritten:
		self.mk2refs.setdefault(r['_marker_key'],[]).append('<reference ref_id="%s"/>'%id)

    def getClass(self,r):
        pass	# override me

    def getLocationRef(self, r):
        return ""	# override me

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

    def processRecord(self, r):
	fclass, soId = self.getClass(r)
	if fclass is None:
	    return None
	r['id'] = self.context.makeItemId('Marker', r['_marker_key'])
	r['featureClass'] = fclass
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
	r['dataSets'] = self.getDataSetRef()
	self.quoteFields(r, ['symbol','name'])
	return r

class MouseFeatureDumper(AbstractFeatureDumper):
    QTMPLT = '''
    SELECT m._organism_key, m._marker_key, m.symbol, m.name, mc._chromosome_key,
	c.term AS mcvtype, a.accid AS primaryidentifier, lc.startcoordinate
    FROM 
	MRK_Marker m LEFT OUTER JOIN 
	MRK_Location_Cache lc 
	    ON m._marker_key = lc._marker_key, 
	MRK_MCV_Cache c, 
	MRK_Chromosome mc, 
	ACC_Accession a
    WHERE m._organism_key = 1
    AND m._marker_status_key = %(OFFICIAL_STATUS)d
    AND m._marker_key = c._marker_key
    AND c.qualifier = 'D'
    AND m.chromosome = mc.chromosome
    AND m._organism_key = mc._organism_key
    AND m._marker_key = a._object_key
    AND a._mgitype_key = %(MARKER_TYPEKEY)d
    AND a._logicaldb_key = %(MGI_LDBKEY)d
    AND a.preferred = 1
    AND a.private = 0
    %(LIMIT_CLAUSE)s
    '''

    def getClass(self, r):
	return MCV2ClassName[r['mcvtype']]

    def getLocationRef(self, r):
	if r['startcoordinate'] is not None:
	    # have to do this without checking - this ref is created *before* 
	    # the location is.
	    ref = self.context.makeGlobalKey('Location', r['_marker_key'])
	    return '<reference name="chromosomeLocation" ref_id="%s" />' % ref
	else:
	    return ''

    def getDataSetRef(self):
        dsid = DataSetDumper(self.context).dataSet(name="Mouse Gene Catalog from MGI")
	return '<reference ref_id="%s"/>'%dsid

class NonMouseFeatureDumper(AbstractFeatureDumper):
    QTMPLT = '''
    SELECT m._organism_key, m._marker_key, m.symbol, m.name, mc._chromosome_key,
	t.name AS mgitype, a.accid AS primaryidentifier
    FROM MRK_Marker m, MRK_Types t, MRK_Chromosome mc, ACC_Accession a
    WHERE m._organism_key in (%(ORGANISMKEYS)s)
    AND m._organism_key != 1
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
    def preDump(self):
        AbstractFeatureDumper.preDump(self)
	self.hasLocation = set()
	q = '''
	    SELECT mf._object_key
	    FROM MAP_Coordinate mc, MAP_Coord_Feature mf
	    WHERE mc._map_key = mf._map_key
	    AND mc._collection_key = %(HUMAN_MAPKEY)d
	'''%self.context.QUERYPARAMS
	for r in self.context.sql(q):
	    self.hasLocation.add(r['_object_key'])

    def getLocationRef(self, r):
        if r['_marker_key'] in self.hasLocation:
            # have to do this without checking - this ref is created *before* 
            # the location is.
            ref = self.context.makeGlobalKey('Location', r['_marker_key'])
            return '<reference name="chromosomeLocation" ref_id="%s" />' % ref
        else:
            return ''

    def getClass(self, r):
	return MCV2ClassName[MGIType2MCVType[r['mgitype']]]

    def getDataSetRef(self):
        dsid = DataSetDumper(self.context).dataSet(
	        name="Non-Mouse Genes from EntrezGene",
		dataSource=self.context.dataSourceByName["Entrez Gene"] )
	return '<reference ref_id="%s"/>'%dsid

# Map from MGI type names to equivalent MCV terms
# (used only for non-mouse features)
MGIType2MCVType = {
    'Gene'			: 'gene',
    'DNA Segment'		: 'DNA segment',
    'Cytogenetic Marker'	: 'cytogenetic marker',
    'QTL'			: 'QTL',
    'Pseudogene'		: 'Pseudogene',
    'BAC/YAC end'		: 'BAC/YAC end',
    'Other Genome Feature'	: 'other genome feature',
    'Complex/Cluster/Region'	: 'complex/cluster/region',
    'Transgene'			: 'transgene',
    }

# Map from MGI MCV terms (_vocab_key=79) to class names in MouseMine
MCV2ClassName = {
    # These are abstract only - nothing should have these types directly.
    #'all feature types'	: None,
    #'cytogenetic marker'	: None,
    #'other feature type'	: None,

    # Mappable types
    'BAC end'			: ('BACEnd',				'SO:0000999'),
    'BAC/YAC end'		: ('BACEnd',				'SO:0000999'),
    'chromosomal deletion'	: ('ChromosomalDeletion',		'SO:1000029'),
    'chromosomal inversion'	: ('ChromosomalInversion',		'SO:1000030'),
    'chromosomal duplication'	: ('ChromosomalDuplication',		'SO:1000037'),
    'chromosomal translocation'	: ('ChromosomalTranslocation',		'SO:1000044'),
    'chromosomal transposition'	: ('ChromosomalTransposition',		'SO:1000453'),
    'complex/cluster/region'	: ('ComplexClusterRegion',		None),
    'DNA segment'		: ('DNASegment',			None),
    'gene'			: ('Gene',				'SO:0000704'),
    'gene segment'		: ('GeneSegment',			'SO:3000000'),
    'heritable phenotypic marker' : ('HeritablePhenotypicMarker',	'SO:0001500'),
    'insertion'			: ('Insertion',				'SO:0000667'),
    'lincRNA gene'		: ('LincRNAGene',			'SO:0001641'),
    'minisatellite'		: ('Minisatellite',			'SO:0000643'),
    'miRNA gene'		: ('MiRNAGene',				'SO:0001265'),
    'non-coding RNA gene'	: ('NcRNAGene',				'SO:0001263'),
    'other genome feature'	: ('OtherGenomeFeature',		None),
    'PAC end'			: ('PACEnd',				'SO:0001480'),
    'polymorphic pseudogene'	: ('PolymorphicPseudogene',		None),
    'protein coding gene'	: ('ProteinCodingGene',			'SO:0001217'),
    'pseudogene'		: ('Pseudogene',			'SO:0000336'),
    'pseudogenic gene segment'	: ('PseudogenicGeneSegment',		None),
    'pseudogenic region'	: ('PseudogenicRegion',			'SO:0000462'),
    'QTL'			: ('QTL',				'SO:0000771'),
    'reciprocal chromosomal translocation' : \
    				  ('ReciprocalChromosomalTranslocation','SO:1000048'),
    'retrotransposon'		: ('Retrotransposon',			'SO:0000180'),
    'RNase MRP RNA gene'	: ('RNaseMRPRNAGene',			'SO:0001640'),
    'RNase P RNA gene'		: ('RNasePRNAGene',			'SO:0001639'),
    'Robertsonian fusion'	: ('RobertsonianFusion',		'SO:1000043'),
    'rRNA gene'			: ('RRNAGene',				'SO:0001637'),
    'scRNA gene'		: ('ScRNAGene',				'SO:0001266'),
    'snoRNA gene'		: ('SnoRNAGene',			'SO:0001267'),
    'snRNA gene'		: ('SnRNAGene',				'SO:0001268'),
    'SRP RNA gene'		: ('SRPRNAGene',			'SO:0001269'),
    'telomere'			: ('Telomere',				'SO:0000624'),
    'telomerase RNA gene'	: ('TelomeraseRNAGene',			'SO:0001643'),
    'transgene'			: ('Transgene',				'SO:0000902'),
    'tRNA gene'			: ('TRNAGene',				'SO:0001272'),
    'unclassified cytogenetic marker'	: \
    				  ('UnclassifiedCytogeneticMarker',	None),
    'unclassified gene'		: ('UnclassifiedGene',			None),
    'unclassified non-coding RNA gene'	: \
    				  ('UnclassifiedNcRNAGene',		None),
    'unclassified other genome feature' : \
    				  ('UnclassifiedOtherGenomeFeature',	None),
    'YAC end'			: ('YACEnd',				'SO:0001498'),
    }

