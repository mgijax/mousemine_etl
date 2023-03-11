
from .AbstractItemDumper import *
from .DataSourceDumper import DataSetDumper
from .DumperContext import DumperContext

class SynonymDumper(AbstractItemDumper):
    QTMPLT = ['''
    /* get allele, strain, etc., synonyms from MGI_Synonyms table */
    SELECT s.synonym, s._object_key, s._mgitype_key
    FROM MGI_Synonym s
    WHERE s._mgitype_key in (%(MGITYPEKEYS)s)
    AND s._mgitype_key != %(MARKER_TYPEKEY)d
    %(LIMIT_CLAUSE)s
    ''','''
    /* get mouse marker synonyms from MRK_Label table */
    SELECT distinct ml.label, ml._marker_key
    FROM MRK_Label ml, MRK_Marker mm
    WHERE ml._orthologorganism_key is null
    AND ml._organism_key in (1,2)
    AND ml.labeltype in ('MS','MN','MY')
    AND ml._marker_key = mm._marker_key
    AND mm._marker_status_key != %(WITHDRAWN_STATUS)d
    %(LIMIT_CLAUSE)s
    ''','''
    /* Secondary ids for markers */
    SELECT a.accid, a._mgitype_key, a._object_key
    FROM ACC_Accession a, MRK_Marker m
    WHERE a._mgitype_key = %(MARKER_TYPEKEY)d
    AND a._logicaldb_key = %(MGI_LDBKEY)d
    AND a.preferred = 0
    AND a.private = 0
    AND a._object_key = m._marker_key
    AND m._marker_status_key != %(WITHDRAWN_STATUS)d
    ''','''
    /* secondary ids for alleles */
    SELECT a.accid, a._mgitype_key, a._object_key
    FROM ACC_Accession a
    WHERE a._mgitype_key = %(ALLELE_TYPEKEY)d
    AND a._logicaldb_key = %(MGI_LDBKEY)d
    AND a.preferred = 0
    AND a.private = 0
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
        try:
            if qindex == 0:
                r['id'] = self.context.makeItemId('Synonym')
                r['value'] = self.quote(r['synonym'])
                r['subject'] = self.context.makeItemRef( r['_mgitype_key'], r['_object_key'])
                return r
            elif qindex == 1:
                r['id'] = self.context.makeItemId('Synonym')
                r['value'] = self.quote(r['label'])
                r['subject'] = self.context.makeItemRef( 'Marker', r['_marker_key'])
                return r
            elif qindex == 2 or qindex == 3:
                # load secondary ids for markers and alleles
                r['id'] = self.context.makeItemId('Synonym')
                r['value'] = self.quote(r['accid'])
                r['subject'] = self.context.makeItemRef( r['_mgitype_key'], r['_object_key'])
                return r
        except DumperContext.DanglingReferenceError as e:
            return None
