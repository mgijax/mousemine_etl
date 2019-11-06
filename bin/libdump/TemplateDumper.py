from .AbstractItemDumper import *


class TemplateDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT
    FROM
    WHERE
    %(LIMIT_CLAUSE)s
        '''
    ITMPLT = '''
    <item class="" id="%(id)s">
      <attribute name="" value="" />
      <reference name="" ref_id="" />
      <collection name="" >
        <reference ref_id="" />
        </collection>
      </item>
    '''

    def preDump(self):
        return True

    def processRecord(self, r):
        return r
        
    def postDump(self):
        pass

