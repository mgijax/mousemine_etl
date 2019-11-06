#
# HomologyDumper.py
#
# Queries MGI for specific homology data sets and dumps them in ItemXML format.
# For every distinct pair of markers m1,m2 in a cluster in MGI, create two 
# Homologue records in MouseMine, one owned by m1 where gene=m1 and homologue=m2, 
# and the other owned by m2 where gene = m2 and homologue = m1.
# Also, if m1.species == m2.species, then type = "paralogue", else "orthologue".
#
# Currently only dumps the hybrid mouse/human homology data set (computed by MGI).
#

from .AbstractItemDumper import *
from .DataSourceDumper import DataSetDumper

class HomologyDumper(AbstractItemDumper):

    DATASETNAME = "Mouse/Human Orthologies from MGI"

    QTMPLT = '''
        SELECT mc._cluster_key, mm._marker_key, mm.symbol, mm._organism_key
        FROM mrk_cluster mc, mrk_clustermember mcm, mrk_marker mm
        WHERE mc._cluster_key = mcm._cluster_key
        AND mcm._marker_key = mm._marker_key
        AND mc._clustersource_key = %(MGI_HYBRID_HOMOLOGY_KEY)d
        ORDER BY mc._cluster_key
        '''

    ITMPLT = '''
        <item class="Homologue" id="%(id)s">
        <attribute name="type" value="%(type)s" />
        <reference name="gene" ref_id="%(gene)s" />
        <reference name="homologue" ref_id="%(homologue)s" />
        <collection name="dataSets"><reference ref_id="%(dataSet)s"/></collection>
        </item>
        '''

    def preDump(self):
        self.currentKey = None
        self.currentCluster = []
        self.dsd = DataSetDumper(self.context)
        self.dsId = self.dsd.dataSet(name=self.DATASETNAME)
        return True

    def flushOnePair(self, a, b):
        htype = "paralogue" if a['_organism_key'] == b['_organism_key']  else "orthologue"
        r = {
            "id" : self.context.makeItemId("Homologue"),
            "type" : htype,
            "gene" : self.context.makeItemRef("Marker", a['_marker_key']),
            "homologue" : self.context.makeItemRef("Marker", b['_marker_key']),
            "dataSet" : self.dsId
            }
        self.writeItem(r, self.ITMPLT)
        
    def flush(self):
        for i in range(len(self.currentCluster)):
            for j in range(i+1, len(self.currentCluster)):
                self.flushOnePair(self.currentCluster[i], self.currentCluster[j])
                self.flushOnePair(self.currentCluster[j], self.currentCluster[i])

    def processRecord(self, r):
        cck = r['_cluster_key']
        if cck == self.currentKey:
            self.currentCluster.append(r)
        else:
            if self.currentKey:
                self.flush()
            self.currentKey = cck
            self.currentCluster = [ r ]
        return None

    def postDump(self):
        self.context.log("PostDump:"+str(self.currentCluster))
        if self.currentKey:
            self.flush()
        
