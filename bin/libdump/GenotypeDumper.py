from .AbstractItemDumper import *

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
      <attribute name="hasMutantAllele" value="%(hasMutantAllele)s" />
      %(note)s
      <collection name="alleles">%(allelerefs)s</collection>
      <collection name="cellLines">%(celllinerefs)s</collection>
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

        # mutant alleles
        self.g2ma = {}
        maquery = '''select distinct g._genotype_key 
                     from gxd_allelepair g, all_allele a1
                     where g._allele_key_1 = a1._allele_key 
                     and a1.iswildtype = 0
                     union
                     select distinct g._genotype_key 
                     from gxd_allelepair g, all_allele a2
                     where g._allele_key_2 = a2._allele_key 
                     and a2.iswildtype  = 0 '''
        for r in self.context.sql(maquery):
            self.g2ma[r['_genotype_key']] = 'true'


        self.gapd = GenotypeAllelePairDumper(self.context)
        self.gapd.dump() # NOTE: caches records; no writes yet (see below)

    def processRecord(self, r):
        gk = r['_genotype_key']
        r['id'] = self.context.makeItemId('Genotype',gk)

        alleleStr = " ".join(['%s/%s'%(x[0],x[1]) for x in self.gapd.gk2pairs.get(gk,[])])

        r['symbol'] = '<attribute name="symbol" value="%s" />'%self.quote(alleleStr) if alleleStr else ''
        r['name'] = self.quote(alleleStr + " [background:] " + r['strain'])
        r['backgroundRef'] = self.context.makeItemRef('Strain', r['_strain_key'])
        r['isconditional'] = r['isconditional'] and "true" or "false"

        r['allelerefs']=''.join(self.makeRefsFromKeys( self.gapd.gk2aks.get(gk,[]), 'Allele' ))
        r['celllinerefs'] = ''.join(self.makeRefsFromKeys( self.gapd.gk2cks.get(gk,[]), 'CellLine' ))
        r['existsAs'] = r['term']
        if r['note']:
            r['note'] = '<attribute name="note" value="%s" />'%self.quote(r['note'])
        else:
            r['note'] = ''
        r['organism'] = self.context.makeItemRef('Organism', 1) # mouse
        r['zygosity'] = self.g2z.get(gk, 'ot')
        r['hasMutantAllele'] = self.g2ma.get(gk,'false')
        return r

    def postDump(self):
        self.gapd.writeRecords()

##################################

class GenotypeAllelePairDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
        p._allelepair_key,
        p._genotype_key, 
        p._allele_key_1, 
        p._allele_key_2, 
        p._MutantCellLine_key_1,
        p._MutantCellLine_key_2,
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
      INNER JOIN
          MRK_Marker m ON p._marker_key = m._marker_key AND m._marker_status_key != 2
    ORDER BY p._genotype_key, p.sequencenum
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="GenotypeAllelePair" id="%(id)s">
      <attribute name="pairState" value="%(pairstate)s" />
      %(genotype)s
      %(feature)s
      %(allele1)s %(mutantCellLine1)s 
      %(allele2)s %(mutantCellLine2)s
      </item>
    '''

    def preDump(self):
        self.gk2mks = {}
        self.gk2aks = {}
        self.gk2cks = {}
        self.gk2pairs = {}
        self.records = []

    def processRecord(self, r):
        gk = r['_genotype_key']
        mk = r['_marker_key']
        ak1 = r['_allele_key_1']
        ak2 = r['_allele_key_2']
        ck1 = r['_mutantcellline_key_1']
        ck2 = r['_mutantcellline_key_2']

        # Special processing. In caching the alleles/markers that a
        # genotype is associated with, do not include things like
        # Gt(ROSA), Cre alleles, Frt,... Here is where we would
        # check...
        if True: # FIXME : check not yet implemented
            s = self.gk2aks.setdefault(gk,set())
            s.add(ak1)
            if ak2:
                s.add(ak2)
            s = self.gk2cks.setdefault(gk,set())
            if ck1:
                s.add(ck1)
            if ck2:
                s.add(ck2)
        #
        pair = (r['allele1'], (r['allele2'] or '?'))
        self.gk2pairs.setdefault(gk, []).append(pair)
        #
        self.records.append(r)
        return None

    def writeRecords(self):
        for r in self.records:
            r['id'] = self.context.makeItemId('GenotypeAllelePair', r['_allelepair_key'])
            self.makeReference(r, '_allele_key_1', 'allele1', 'Allele' )
            self.makeReference(r, '_mutantcellline_key_1', 'mutantCellLine1', 'CellLine' )
            self.makeReference(r, '_mutantcellline_key_2', 'mutantCellLine2', 'CellLine' )
            self.makeReference(r, '_allele_key_2', 'allele2', 'Allele' )
            self.makeReference(r, '_genotype_key', 'genotype','Genotype')
            self.makeReference(r, '_marker_key',   'feature', 'Marker')
            self.writeItem(r)
        
