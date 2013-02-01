from AbstractItemDumper import *

class GenotypeDumper(AbstractItemDumper):
    QTMPLT= '''
    SELECT g._genotype_key, g._strain_key, s.strain, g.isconditional, g.note, t.term, a.accid
    FROM GXD_Genotype g, VOC_Term t, PRB_Strain s, ACC_Accession a
    WHERE g._existsas_key = t._term_key
    AND g._strain_key = s._strain_key
    AND g._genotype_key = a._object_key
    AND a._logicaldb_key = %(MGI_LDBKEY)s
    AND a._mgitype_key = %(GENOTYPE_TYPEKEY)s
    AND a.preferred = 1
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Genotype" id="%(id)s" >
      <attribute name="primaryIdentifier" value="%(accid)s" />
      %(symbol)s
      <attribute name="zygosity" value="%(zygosity)s" />
      <attribute name="name" value="%(name)s" />
      <reference name="organism" ref_id="%(organism)s" />
      <reference name="background" ref_id="%(backgroundRef)s" />
      <attribute name="isConditional" value="%(isconditional)s" />
      <attribute name="existsAs" value="%(existsAs)s" />
      %(note)s<collection name="alleles">%(allelerefs)s</collection>
      </item>
    '''

    def preDump(self):
	self.g2z = {}
	def doQ(qry, label):
	    for r in self.context.sql(qry):
		self.g2z[r['_genotype_key']] = label

	# homozygous
        doQ( '''select _genotype_key
                from gxd_allelepair
                where _Compound_key = 847167 and _PairState_key = 847138''',
		'hm')
                
	# heterozygous
        doQ( '''select _genotype_key
                from gxd_allelepair
                where _Compound_key = 847167 and _PairState_key = 847137''',
		'ht')

	# complex
        doQ( '''select _genotype_key, count(1)
                from gxd_allelepair
                group by _genotype_key
                having count(1) > 1''',
		'cx')

	# transgenic
        doQ( '''select distinct g._genotype_key
                from gxd_allelegenotype g, all_allele a
                where g._Allele_key = a._Allele_key
		and a._Allele_Type_key in (847127, 847128, 847129, 2327160)''',
		'tg')

	# conditional
        doQ( '''select _genotype_key
		from gxd_genotype
                where isConditional = 1''',
		'cn')

        self.gapd = GenotypeAllelePairDumper(self.context)
	self.gapd.dump() # NOTE: caches records; no writes yet (see below)

    def processRecord(self, r):
	gk = r['_genotype_key']
	r['id'] = self.context.makeItemId('Genotype',gk)

	alleleStr = " ".join(map( lambda x: '%s/%s'%(x[0],x[1]), self.gapd.gk2pairs.get(gk,[])))

	r['symbol'] = '<attribute name="symbol" value="%s" />'%self.quote(alleleStr) if alleleStr else ''
	r['name'] = self.quote(alleleStr + " [background:] " + r['strain'])
	r['backgroundRef'] = self.context.makeItemRef('Strain', r['_strain_key'])
	r['isconditional'] = r['isconditional'] and "true" or "false"

	r['allelerefs']=''.join(self.makeRefsFromKeys( self.gapd.gk2aks.get(gk,[]), 'Allele' ))
	r['existsAs'] = r['term']
	if r['note']:
	    r['note'] = '<attribute name="note" value="%s" />'%self.quote(r['note'])
	else:
	    r['note'] = ''
	r['organism'] = self.context.makeItemRef('Organism', 1) # mouse
	r['zygosity'] = self.g2z.get(gk, 'ot')
	return r

    def postDump(self):
        self.gapd.writeRecords()

##################################

class GenotypeAllelePairDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
        p._genotype_key, 
	p._allele_key_1, 
	p._allele_key_2, 
	p._marker_key, 
	t.term AS pairstate,
        a1.symbol AS allele1, 
	a2.symbol AS allele2
    FROM GXD_AllelePair p 
      INNER JOIN
          ALL_Allele a1 ON p._allele_key_1 = a1._allele_key
      LEFT OUTER JOIN 
          ALL_Allele a2 ON p._allele_key_2 = a2._allele_key
      INNER JOIN
          VOC_Term t ON p._pairstate_key = t._term_key
    ORDER BY p._genotype_key, p.sequencenum
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="GenotypeAllelePair" id="%(id)s">
      <attribute name="pairState" value="%(pairstate)s" />
      <reference name="genotype" ref_id="%(genotypeid)s" />
      <reference name="allele1" ref_id="%(allele1id)s" />
      %(allele2ref)s
      %(featureRef)s
      </item>
    '''

    def preDump(self):
	self.gk2mks = {}
        self.gk2aks = {}
	self.gk2pairs = {}
	self.records = []

    def processRecord(self, r):
	mk = r['_marker_key']
	ak1 = r['_allele_key_1']
	ak2 = r['_allele_key_2']
	gk = r['_genotype_key']

	# Special processing. In caching the alleles/markers that a
	# genotype is associated with, do not include things like
	# Gt(ROSA), Cre alleles, Frt,... Here is where we would
	# check...
	if True: # FIXME : check not yet implemented
	    s = self.gk2aks.setdefault(gk,set())
	    s.add(ak1)
	    if ak2:
		s.add(ak2)
	#
	pair = (r['allele1'], (r['allele2'] or '?'))
	self.gk2pairs.setdefault(gk, []).append(pair)
	#
	self.records.append(r)
	return None

    def writeRecords(self):
        for r in self.records:
	    try:
		r['id'] = self.context.makeItemId('GenotypeAllelePair')
		r['allele1id'] = self.context.makeItemRef('Allele', r['_allele_key_1'])
		r['allele2ref'] = ''
		if r['_allele_key_2']:
		    r['allele2ref'] = '<reference name="allele2" ref_id="%s" />'  \
			% self.context.makeItemRef('Allele', r['_allele_key_2'])
		r['genotypeid'] = self.context.makeItemRef('Genotype', r['_genotype_key'])
		if r['_marker_key']:
		    r['featureRef'] = '<reference name="feature" ref_id="%s" />' \
			% self.context.makeItemRef('Marker', r['_marker_key'])
                else:
                    r['featureRef'] = ''
	    except DumperContext.DanglingReferenceError:
	        pass
	    else:
		self.writeItem(r)
	
