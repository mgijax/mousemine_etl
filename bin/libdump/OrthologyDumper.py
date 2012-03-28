from AbstractItemDumper import *
from DataSourceDumper import DataSetDumper

class OrthologyDumper(AbstractItemDumper):

    QTMPLT = ['''
     SELECT _assay_key AS _code_key, assay AS name, abbrev
     FROM HMD_Assay
     ORDER BY abbrev
     %(LIMIT_CLAUSE)s
    ''' , '''
     SELECT DISTINCT h._class_key, m._marker_key, m._organism_key
     FROM HMD_Homology h, HMD_Homology_Marker hm, MRK_Marker m
     WHERE h._homology_key = hm._homology_key
     AND hm._marker_key = m._marker_key
     AND m._organism_key in (%(ORGANISMKEYS)s)
     AND m._marker_status_key = %(OFFICIAL_STATUS)d
     ORDER BY h._class_key, m._organism_key
     %(LIMIT_CLAUSE)s
    ''','''
     SELECT DISTINCT h._class_key, h._refs_key, a._assay_key
     FROM HMD_Homology h, HMD_Homology_Assay a
     WHERE h._homology_key = a._homology_key
     ORDER BY h._class_key, a._assay_key
     %(LIMIT_CLAUSE)s
    '''
    ]
    ITMPLT = ['''
    <item class="OrthologueEvidenceCode" id="%(id)s">
      <attribute name="name" value="%(name)s" />
      <attribute name="abbreviation" value="%(abbrev)s" />
      </item>
    ''',
    
    # Intermine note: Use British spelling!!
    '''
    <item class="Homologue" id="%(id)s">
      <attribute name="type" value="orthologue"/>
      <reference name="gene" ref_id="%(geneid)s"/>
      <reference name="homologue" ref_id="%(hgeneid)s"/>
      <collection name="evidence"> %(evidenceRefs)s </collection>
      <collection name="dataSets">%(dataSets)s</collection>
      </item>
    ''','''
    <item class="OrthologueEvidence" id="%(id)s">
      <reference name="evidenceCode" ref_id="%(codeid)s" />
      <collection name="publications"> %(pubRefs)s </collection>
      </item>
    ''']

    def preDump(self):
	self.hdata = {}
	self.hevidence = {}
	self.dsid =  \
	    DataSetDumper(self.context).dataSet(name="Mammalian Orthology Associations from MGI")

    def processRecord(self, r, i):
	# delegate to the i-th processing method.
	return getattr(self, 'process%d'%i)(r)

    def process0(self, r):
	# Processes a HMD_Assay record. Just write out the
	# corresponding OrthologueEvidenceCode item.
	r['id'] = \
	    self.context.makeItemId('OrthologueEvidenceCode', r['_code_key'])
	return r

    def process1(self, r):
	# Accumulate all genes in the HomologyClass,
	# but don't write anything yet.)
	# Build hdata, : 
	#	{classkey->[markerkeys]} 
	ck = r['_class_key']
	mk = self.context.makeItemRef('Marker', r['_marker_key'])
	self.hdata.setdefault(ck,[]).append(mk)

    def process2(self, r):
	# InterMine data model wants one evidence record PER evidence code, where
	# each record may have several references. Accumulate evidence info
	# into hevidence:
	# 	{classkey->{assaykey->[pubkeys]}}
	ck = r['_class_key']
	rk = self.context.makeItemRef('Reference', r['_refs_key'])
	if self.hdata.has_key(ck):
	    ak = self.context.makeItemRef('OrthologueEvidenceCode', r['_assay_key'])
	    self.hevidence.setdefault(ck,{}).setdefault(ak,[]).append(rk)

    def postDump(self):
	erefs = {}
	for ck, codedict in self.hevidence.iteritems():
	    for codekey, refkeys in codedict.iteritems():
		eid = self.context.makeItemId('OrthologueEvidence')
		erefs.setdefault(ck,[]).append('<reference ref_id="%s" />'%eid)
		pubRefs = []
		for rk in refkeys:
		    pubRefs.append( '<reference ref_id="%s" />' % rk )
	        r = { 'id' : eid, 'codeid' : codekey, 'pubRefs' : ''.join(pubRefs), }
		self.writeItem(r, self.ITMPLT[2])

	dsets = '<reference ref_id="%s"/>'%self.dsid
	for ck,mks in self.hdata.iteritems():
	    ers = ''.join(erefs.get(ck,[]))
	    for mk in mks:
	        for hk in mks:
		    if mk != hk:
			r = {
			    'id'	: self.context.makeItemId('Homologue'),
			    'geneid'	: mk,
			    'hgeneid'	: hk,
			    'evidenceRefs'	: ers,
			    'dataSets'	: dsets,
			    }
		        self.writeItem(r, self.ITMPLT[1])


