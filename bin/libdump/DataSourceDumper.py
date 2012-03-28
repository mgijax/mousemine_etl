
from AbstractItemDumper import *

class DataSourceDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT db._logicaldb_key, db.name, db.description
    FROM ACC_LogicalDB db
    %(LIMIT_CLAUSE)s
    '''

    ITMPLT = '''
    <item class="DataSource" id="%(id)s">
      <attribute name="name" value="%(name)s" />
      <attribute name="description" value="%(description)s" />
      </item>
    '''

    def preDump(self):
        self.context.dataSourceByName = {}

    def processRecord(self, r):
	r['id'] = self.context.makeItemId('DataSource', r['_logicaldb_key'])
	self.context.dataSourceByName[r['name']] = r['id']
	self.quoteFields(r, ['name','description'])
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



