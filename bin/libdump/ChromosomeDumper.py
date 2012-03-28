from AbstractItemDumper import *

class ChromosomeDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT c._organism_key, c.chromosome, o.commonname, mc._chromosome_key, max(c.endcoordinate) AS length
    FROM mrk_location_cache c, mgi_organism o, mrk_chromosome mc
    WHERE c._organism_key = o._organism_key
    AND c._organism_key = mc._organism_key
    AND c.chromosome = mc.chromosome
    GROUP BY c._organism_key, c.chromosome, o.commonname, mc._chromosome_key
    ORDER BY c._organism_key, c.chromosome
    '''
    ITMPLT = '''
    <item class="Chromosome" id="%(id)s" >
      <attribute name="primaryIdentifier" value="%(primaryIdentifier)s" />
      <reference name="sequenceOntologyTerm" ref_id="%(soterm)s" />
      <attribute name="symbol" value="%(symbol)s" />
      <attribute name="name" value="%(name)s" />
      <attribute name="length" value="%(length)d" />
      <reference name="organism" ref_id="%(organism)s" />
      </item>
    '''

    def processRecord(self, r):
	n = r['commonname']
	i = n.find(",")
	if i != -1:
	    n = n[:i]
	r['id'] = self.context.makeItemId('Chromosome', r['_chromosome_key'])
	r['symbol'] = 'chr'+r['chromosome']
	r['name'] = 'Chromosome %s (%s)' % (r['chromosome'], n)
	r['primaryIdentifier'] = r['chromosome']
	r['organism'] = self.context.makeItemRef('Organism', r['_organism_key'])
	r['soterm'] = self.context.makeGlobalKey('SOTerm', 340)
	if r['length'] is None:
	    r['length'] = 0
	return r

