
from AbstractItemDumper import *


class CrossReferenceDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT a._accession_key, a.accid, a._logicaldb_key, a._object_key, a._mgitype_key
    FROM ACC_Accession a
    %(WHERECLAUSE)s 
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="CrossReference" id="%(id)s">
      <attribute name="identifier" value="%(identifier)s" />
      <reference name="subject" ref_id="%(subject)s" />
      <reference name="source" ref_id="%(source)s" />
      </item>
    '''
    def __init__(self, context, mgiTypeKeys=[1,2,10,11], ldbKeys=None, notLdbKeys=[1] ):
        AbstractItemDumper.__init__(self, context)

	def fmt(ks):
	    return ",".join(map(str,ks))

	clauses = []
	if mgiTypeKeys :
	    clauses.append('a._mgitype_key in (%s)'%fmt(mgiTypeKeys))
	if ldbKeys:
	    clauses.append('a._logicaldb_key in (%s)'%fmt(ldbKeys))
	if notLdbKeys:
	    clauses.append('a._logicaldb_key not in (%s)'%fmt(notLdbKeys))
	self.whereClause = ''
	if clauses:
	    self.whereClause = 'WHERE %s' % (' AND '.join(clauses))
        
    def preDump(self):
        self.context.QUERYPARAMS['WHERECLAUSE'] = self.whereClause

    def processRecord(self, r):
	r['id'] = self.context.makeItemId('CrossReference', r['_accession_key'])
	r['identifier'] = self.quote(r['accid'])
	r['subject'] = self.context.makeItemRef(r['_mgitype_key'], r['_object_key'])
	r['source'] = self.context.makeItemRef('DataSource', r['_logicaldb_key'])
	return r
