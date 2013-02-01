from AbstractItemDumper import *

class CellLineDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
	cl._cellline_key,
	cl.cellLine AS name,
	cl.isMutant,
	clt.term AS celllinetype,
	cl._strain_key,
	cl._derivation_key
    FROM 
        ALL_CellLine cl, 
	VOC_Term  clt
    WHERE
	cl._CellLine_Type_key = clt._term_key

    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="CellLine" id="%(id)s">
      <attribute name="name" value="%(name)s" />
      <attribute name="cellLineType" value="%(celllinetype)s" />
      <reference name="strain" ref_id="%(strainRef)s" />
      %(derivationRef)s
      <collection name="alleles">%(alleleRefs)s</collection>
      </item>
    '''

    def preDump(self):
        self.ck2ars = {}
	q = "SELECT _allele_key, _mutantcellline_key FROM ALL_Allele_CellLine"
	for r in self.context.sql(q):
	    ck = r['_mutantcellline_key']
	    ak = r['_allele_key']
	    try:
	        ar = '<reference ref_id="%s" />'%self.context.makeItemRef('Allele', ak)
	        self.ck2ars[ck] = self.ck2ars.get(ck,'')+ar
	    except DumperContext.DanglingReferenceError:
		pass

    def processRecord(self, r):
	r['id'] = self.context.makeItemId('CellLine', r['_cellline_key'])
	r['name'] = self.quote(r['name'])
	if r['celllinetype'] == "Embryonic Stem Cell":
	    r['celllinetype'] = "ES Cell"
	r['strainRef'] = self.context.makeItemRef('Strain', r['_strain_key'])
	if r['_derivation_key']:
	    # these refs are generated before the derivation objects are writted, so must avoid checks
	    dr = self.context.makeGlobalKey('CellLineDerivation', r['_derivation_key'])
	    r['derivationRef'] = '<reference name="derivation" ref_id="%s" />' % dr
	else:
	    r['derivationRef'] = ''
	r['alleleRefs'] = self.ck2ars.get(r['_cellline_key'],'')
	
        return r

    def postDump(self):
        CellLineDerivationDumper(self.context).dump()

class CellLineDerivationDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
	cld._derivation_key,
	cld.name,
	v.term AS vector,
	vt.term AS vectortype,
	cld._parentcellline_key,
	dt.term AS derivationtype,
	c.term AS creator,
	cld._refs_key
    FROM 
	All_CellLine_Derivation cld,
	VOC_Term v,
	VOC_Term vt,
	VOC_Term dt,
	VOC_Term c
    WHERE
	cld._vector_key = v._term_key
    AND cld._vectortype_key = vt._term_key
    AND cld._derivationtype_key = dt._term_key
    AND cld._creator_key = c._term_key

    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="CellLineDerivation" id="%(id)s">
      <attribute name="name" value="%(name)s" />
      <attribute name="vector" value="%(vector)s" />
      <attribute name="vectorType" value="%(vectortype)s" />
      <attribute name="derivationType" value="%(derivationtype)s" />
      <attribute name="creator" value="%(creator)s" />
      <reference name="parentCellLine" ref_id="%(parentcellline)s" />
      %(pubRef)s
      </item>
    '''

    def processRecord(self, r):
	r['id'] = self.context.makeItemId('CellLineDerivation', r['_derivation_key'])
	r['parentcellline'] = self.context.makeItemRef('CellLine', r['_parentcellline_key'])
	r['name'] = self.quote(r['name'])
	r['vector'] = self.quote(r['vector'])
	r['creator'] = self.quote(r['creator'])
	if r['_refs_key']:
	    pr = self.context.makeItemRef('Reference', r['_refs_key'])
	    r['pubRef'] = '<reference name="publication" ref_id="%s" />' % pr
	else:
	    r['pubRef'] = ''
        return r
