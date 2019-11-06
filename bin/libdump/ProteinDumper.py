
from .AbstractItemDumper import *

class ProteinDumper(AbstractItemDumper):
    QTMPLT = '''
        SELECT distinct mc._organism_key, mc.accid, mc._marker_key
        FROM SEQ_Marker_Cache mc
        WHERE mc._logicaldb_key in (%(SP_LDBKEY)d,%(TR_LDBKEY)d)
        AND mc._organism_key = %(MOUSE_ORGANISMKEY)d
        AND mc._marker_type_key = %(GENE_MRKTYPEKEY)d
        ORDER BY mc.accid
        '''
    ITMPLT = '''
    <item class="Protein" id="%(id)s">
      <attribute name="primaryAccession" value="%(primaryAccession)s" />
      <reference name="organism" ref_id="%(organism)s"/>
      <collection name="genes">%(genes)s</collection>
      </item>
    '''

    def preDump(self):
        self.currAcc = None
        self.currOk = None
        self.currMrefs = []
        return True

    def flushCurr(self):
        if self.currAcc:
            r = {}
            r['id'] = self.context.makeItemId('Sequence')
            r['primaryAccession'] = self.currAcc
            r['genes'] = ''.join(self.currMrefs)
            r['organism'] = self.context.makeItemRef('Organism', self.currOk)
            self.writeItem(r, self.ITMPLT)

    def processRecord(self, r):
        pid = r['accid']
        mk  = r['_marker_key']
        mref = '<reference ref_id="%s"/>' % self.context.makeItemRef('Marker', mk)
        ok = r['_organism_key']
        if pid == self.currAcc:
            self.currMrefs.append(mref)
        else:
            self.flushCurr()
            self.currAcc = pid
            self.currOk = ok
            self.currMrefs = [mref]
        return None
        
    def postDump(self):
        self.flushCurr()
