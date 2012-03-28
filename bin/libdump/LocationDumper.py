from AbstractItemDumper import *


class LocationDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT c._marker_key, mc._chromosome_key, c.chromosome, c.startcoordinate, c.endcoordinate, c.strand
    FROM MRK_Location_Cache c, MRK_Chromosome mc
    WHERE c.chromosome = mc.chromosome
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


class AbstractLocationDumper(AbstractItemDumper):
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

class MouseLocationDumper(AbstractLocationDumper):
    QTMPLT = '''
    SELECT c._marker_key, mc._chromosome_key,
	   c.startcoordinate, c.endcoordinate, c.strand
    FROM MRK_Location_Cache c, MRK_Chromosome mc, MRK_Marker m, ACC_Accession a
    WHERE  c.startcoordinate is not null
    AND c.chromosome = mc.chromosome
    AND mc._organism_key = 1
    AND c._marker_key = m._marker_key
    AND m._marker_status_key = %(OFFICIAL_STATUS)d
    AND m._marker_key = a._Object_key
    AND a._mgitype_key = %(MARKER_TYPEKEY)d
    AND a._logicaldb_key = %(MGI_LDBKEY)d
    AND a.preferred = 1
    AND a.private = 0
    %(LIMIT_CLAUSE)s
    '''

class NonMouseLocationDumper(AbstractLocationDumper):
    QTMPLT = '''
    SELECT mf._object_key AS _marker_key, mc._object_key AS _chromosome_key, 
        mf.startCoordinate, mf.endCoordinate, mf.strand
    FROM MAP_Coordinate mc, MAP_Coord_Feature mf
    WHERE mc._map_key = mf._map_key
    AND mc._collection_key = %(HUMAN_MAPKEY)d
    %(LIMIT_CLAUSE)s
    '''
