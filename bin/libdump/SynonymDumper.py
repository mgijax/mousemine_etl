
from AbstractItemDumper import *

class SynonymDumper(AbstractItemDumper):
    QTMPLT = ['''
    SELECT s.synonym, s._object_key, s._mgitype_key
    FROM MGI_Synonym s
    WHERE s._mgitype_key in (%(MGITYPEKEYS)s)
    AND s._mgitype_key != 2
    %(LIMIT_CLAUSE)s
    ''','''
    SELECT ml.label, ml._marker_key
    FROM MRK_Label ml
    WHERE ml._orthologorganism_key is null
    AND ml.labeltype in ('MS','MN','MY')
    %(LIMIT_CLAUSE)s
    ''']

    ITMPLT = '''
    <item class="Synonym" id="%(id)s">
      <attribute name="value" value="%(value)s" />
      <reference name="subject" ref_id="%(subject)s" />
      </item>
    '''

    def __init__(self, context, mgiTypeKeys=[2,10,11]):
	AbstractItemDumper.__init__(self,context)
	self.mgiTypeKeys = mgiTypeKeys

    def preDump(self):
	self.context.QUERYPARAMS['MGITYPEKEYS'] = ",".join(map(str,self.mgiTypeKeys))

    def processRecord(self, r, qindex):
	if qindex == 0:
	    r['id'] = self.context.makeItemId('Synonym')
	    r['value'] = self.quote(r['synonym'])
	    r['subject'] = self.context.makeItemRef( r['_mgitype_key'], r['_object_key'])
	    return r
	else:
	    r['id'] = self.context.makeItemId('Synonym')
	    r['value'] = self.quote(r['label'])
	    r['subject'] = self.context.makeItemRef( 'Marker', r['_marker_key'])
	    return r

