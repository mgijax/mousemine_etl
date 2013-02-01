from AbstractItemDumper import *


class LocationDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT c._marker_key, mc._chromosome_key, c.startcoordinate, c.endcoordinate, c.strand
    FROM MRK_Location_Cache c, MRK_Chromosome mc
    WHERE c.genomicchromosome = mc.chromosome
    AND c._organism_key = mc._organism_key
    AND c.startCoordinate is not null
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Location" id="%(id)s">
      <reference name="feature" ref_id="%(markerid)s" />
      <reference name="locatedOn" ref_id="%(chromosomeid)s" />
      <attribute name="start" value="%(startcoordinate)d" />
      <attribute name="end" value="%(endcoordinate)d" />
      <attribute name="strand" value="%(strand)s" />
      </item>
    '''

    def processRecord(self, r):
	r['id'] = self.context.makeItemId('Location', r['_marker_key'])
	r['markerid'] = self.context.makeItemRef('Marker', r['_marker_key'])
	r['chromosomeid'] = self.context.makeItemRef('Chromosome', r['_chromosome_key'])
	# Intermine note: standard is for strand to be "+1" or "-1"
	if r['strand'] in ["+","-"]:
	    r['strand'] += '1'
	else:
	    r['strand'] = '0'
	# Sanity checks.
	if r['startcoordinate'] > r['endcoordinate']:
	    self.context.log('\nLocation: start > end:\n%s\n'%str(r))
	    r['startcoordinate'], r['endcoordinate'] = r['endcoordinate'], r['startcoordinate']
	if r['startcoordinate'] == 0:
	    self.context.log('\nLocation: start == 0:\n%s\n'%str(r))
	    r['startcoordinate'] = 1
        return r
