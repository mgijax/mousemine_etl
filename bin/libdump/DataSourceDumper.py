
from AbstractItemDumper import *

class DataSourceDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT db._logicaldb_key, db.name, db.description, ab.name AS aname, ab.url
    FROM ACC_LogicalDB db
      LEFT OUTER JOIN ACC_ActualDB ab
      ON db._logicaldb_key = ab._logicaldb_key
    ORDER BY db._logicaldb_key, ab._actualdb_key
    %(LIMIT_CLAUSE)s
    '''

    ITMPLT = '''
    <item class="DataSource" id="%(id)s">
      <attribute name="name" value="%(name)s" />
      <attribute name="description" value="%(description)s" />
      %(url)s
      </item>
    '''

    def preDump(self):
        self.context.dataSourceByName = {}
	q = '''
	SELECT _logicaldb_key
	FROM ACC_ActualDB
	GROUP BY _logicaldb_key
	HAVING count(*) > 1
	'''
	self.multActual = set()
	for r in self.context.sql(q):
	    self.multActual.add(r['_logicaldb_key'])

    def processRecord(self, r):
	try:
	    r['id'] = self.context.makeItemId('DataSource', r['_logicaldb_key'])
	except:
	    return None
	if r['_logicaldb_key'] in self.multActual:
	    r['name'] = r['aname']
	if r['name'] == "MGI":
	    r['url'] = 'http://www.informatics.jax.org/accession/@@@@'
	if r['url'] is None:
	    r['url'] = ''
	else:
	    r['url'] = '<attribute name="url" value="%s" />' % self.quote(r['url'])
        r['url'] = r['url'].replace('@@@@','&lt;&lt;attributeValue&gt;&gt;')
	self.context.dataSourceByName[r['name']] = r['id']
	r['name'] = self.quote(r['name'])
	r['description'] = self.quote(r['description'])
	return r

class DataSetDumper(AbstractItemDumper):
    ITMPLT = '''
    <item class="DataSet" id="%(id)s">
	<attribute name="name" value="%(name)s" />
	<reference name="dataSource" ref_id="%(dataSource)s" />
	</item>
    '''
    def dataSet(self, **rec):
	if not hasattr(self.context, 'dataSetByName'):
	    self.context.dataSetByName = {}
	n = rec['name']
	id = self.context.dataSetByName.get(n, None)
	if id:
	    return id
        id = self.context.makeItemId('DataSet')
	rec['id'] = id
	if not rec.has_key('dataSource'):
	    rec['dataSource'] = self.context.makeItemRef('DataSource',1)
	self.writeItem(rec)
	self.context.dataSetByName[n] = id
	return id



