from AbstractItemDumper import *


class StrainDumper(AbstractItemDumper):
    QTMPLT='''
    SELECT a.accid, s._strain_key, s.strain AS name, t.term AS straintype, s.standard
    FROM
      PRB_Strain s JOIN VOC_Term t
      ON s._straintype_key = t._term_key
    LEFT OUTER JOIN ACC_Accession a
      ON s._strain_key = a._object_key
      AND a._mgitype_key = %(STRAIN_TYPEKEY)s
      AND a._logicaldb_key = 1
      AND a.preferred = 1
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Strain" id="%(id)s" >
      <reference name="organism" ref_id="%(organism)s" />
      <attribute name="primaryIdentifier" value="%(accid)s" />
      <attribute name="name" value="%(name)s" />
      <attribute name="strainType" value="%(straintype)s" />
      <collection name="publications">%(publications)s</collection>
      </item>
    '''
    def loadStrainPubs(self):
        self.sk2pk = {}
	q='''
	SELECT ra._refs_key, ra._object_key as "_strain_key"
	FROM MGI_Reference_Assoc ra
	WHERE ra._refassoctype_key in (%s)
	''' % ','.join([ str(x) for x in self.context.QUERYPARAMS['STRAIN_REFASSOCTYPE_KEYS']])
	for r in self.context.sql(q):
	    self.sk2pk.setdefault( r['_strain_key'], []).append(self.context.makeItemRef('Reference', r['_refs_key']))

    def preDump(self):
        self.loadStrainPubs()

    def processRecord(self, r):
	sk = r['_strain_key']
	r['id'] = self.context.makeItemId('Strain', sk)
	r['organism'] = self.context.makeItemRef('Organism', 1) # mouse
	r['name'] = self.quote(r['name'])
	r['straintype'] = self.quote(r['straintype'])
	r['publications'] = ''.join(['<reference ref_id="%s"/>'%x for x in self.sk2pk.get(sk,[])])
        return r

