from AbstractItemDumper import *

class OrganismDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT a.accid, o._organism_key
    FROM MGI_Organism o, ACC_Accession a
    WHERE o._organism_key = a._object_key
    AND a._mgitype_key = %(ORGANISM_TYPEKEY)d
    AND a._logicaldb_key = %(TAXONOMY_LDBKEY)d
    AND a._object_key in (%(ORGANISMKEYS)s)
    ORDER BY o._organism_key
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Organism" id="%(id)s">
       <attribute name="taxonId" value="%(accid)s" />
       </item>
    '''
    def processRecord(self, r):
	r['id'] = self.context.makeItemId('Organism', r['_organism_key'])
	return r
