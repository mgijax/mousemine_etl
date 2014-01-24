#!/usr/bin/env python
#
# fmfd.py (Friendly Mine Feature Dumper)
#
# Purpose: to fill in basic data about genes for other organisms in the InterMOD consortium.
# This includes primary ID, symbol, name, and chromosome location (if available). 
#
# The basic outline: 
# 1. Get the data from the other mines! (Why not?) All the info is from the core model,
# it's simple stuff, so it should be easy to do. Should be able to run the same queries
# at each mine. And of course, InterMine itself makes the mechanics of doing all this simple.
# 2. Generate ItemXML. Make this part of our data preparation (ETL) step.
# 3. Separate source for each species. This is not strictly necessary, but it make key management
#	a WHOLE lot easier. 
#

import sys
import os
import urllib
import json
import time
import types

LIMIT = ""

MINES = {
    "mouse": { 
	"name" : "MouseMine",
        "url" : "http://www.mousemine.org/mousemine", 
	"taxon" : 10090,
    },
    "fly" : { 
	"name" : "FlyMine",
        "url" : "http://www.flymine.org/release-38.0", 
	"taxon" : 7227,
    },
    "rat" : { 
	"name" : "RatMine",
        "url" : "http://ratmine.mcw.edu/ratmine", 
	"taxon" : 10116,
    },
    "yeast" : { 
	"name" : "YeastMine",
        "url" : "http://yeastmine.yeastgenome.org/yeastmine", 
	"taxon" : 4932,
    },
    "zebrafish" : { 
	"name" : "ZebraFishMine",
        "url" : "http://zmine.zfin.org", 
	"taxon" : 7955,
    },
    "worm" : { 
	"name" : "WormMine",
        "url" : "http://www.wormbase.org/tools/wormmine", 
	"taxon" : 6239,
    }}

#
class FriendlyMineFeatureDumper:
    def __init__(self, **cfg):
        self.name = cfg.get('name', None)
	self.url = cfg.get('url', None)
	self.taxon = cfg.get('taxon', None)
	self.file = cfg.get('file', None)
	self.date = time.asctime(time.localtime(time.time()))
	self.description = self.name
	self.ofd = None
	self.dataSourceId = None
	self.dataSetId = None

    def iql(self, q):
	url = self.url+"/service/query/results?format=json&start=0"+ \
	      LIMIT+"&query="+urllib.quote(q)
	fd = urllib.urlopen(url)
	o=json.load(fd)
        fd.close()
	return o

    def mkRef(self, type, id):
        return "%d_%d" % (self.TYPEKEYS[type], id)

    def cleanse(self, s):
        return s.replace("&","&amp;").replace('"', "&quot;").replace("<","&lt;")

    def mkAttr(self, name, value):
        if value is None:
	    return ''
	else:
	    try:
	        v = str(value)
	    except:
		v = value.encode('ascii','ignore')
	    return '<attribute name="%s" value="%s" />' % (name, self.cleanse(v))

    def get(self, name):
        q = self.QTMPLTS.get(name, None)
	if not q: return None
	return self.iql(q%self.taxon)

    def dumpDataSource(self):
	self.dataSourceId = self.mkRef("DataSource", 1)
	self.ofd.write( self.ITMPLTS["DataSource"] % {
	    "id" : self.dataSourceId,
	    "name" : self.name,
	    "url" : self.cleanse(self.url),
	    "description" : self.name,
	    })

    def dumpDataSet(self):
	self.dataSetId = self.mkRef("DataSet", 1)
	self.ofd.write( self.ITMPLTS["DataSet"] % {
	    "id" : self.dataSetId,
	    "name" : "Basic Gene Info",
	    "version" : self.date,
	    "description" : "Gene symbols, names, and genome coordinates. Downloaded via web service API.",
	    "dataSource" : self.mkRef("DataSource", 1),
	    })

    def dumpOrganism(self):
	n = "Organism"
        r = self.get(n)["results"][0]
	self.ofd.write( self.ITMPLTS[n] % { 
	    "id" : self.mkRef(n, r[0]),
	    "taxon" : r[1],
	  })

    def dumpChromosomes(self):
        n = "Chromosome"
	chrs = self.get(n)
	for c in chrs["results"]:
	    if c[3] is None:
	        c[3] = self.maxLens.get(c[0],None)
	    self.ofd.write(self.ITMPLTS[n] % {
		"id" : self.mkRef(n, c[0]),
		"primaryIdentifier" : c[1] and c[1] or c[2],
		"organism" : self.mkRef("Organism", c[4]),
		"length" : self.mkAttr("length", c[3]),
	        })

    def dumpGenes(self):
        n = "Gene"
	self.gids = set()
	genes = self.get(n)
	igenes = []
	# Even though we query for the SOTerms attached to the Genes, we're going to ignore them
	# for now and force everything to have the gene SO term. This is because of variations in how
	# SOTerms are used and extended by different mines.
	self.ofd.write(self.ITMPLTS["SOTerm"] % {
	    "id" : self.mkRef("SOTerm", 1),
	    "identifier" : "SO:0000704",
	    })
	#
	for g in genes["results"]:
	    cloc = ''
	    if g[5]:
	        cloc = '<reference name="chromosomeLocation" ref_id="%s" />'%self.mkRef("Location", g[5])
	    self.gids.add(g[0])
	    self.ofd.write(self.ITMPLTS[n] % {
		"id" : self.mkRef(n, g[0]),
		"primaryIdentifier" : g[1],
		"symbol" : g[2],
		"name" : self.mkAttr("name", g[3]),
		"organism" : self.mkRef("Organism", g[4]),
		"chromosomeLocation" : cloc,
		"sequenceOntologyTerm" : self.mkRef("SOTerm", 1),
		"dataSet" : self.dataSetId,
	        })

    def dumpLocations(self):
        n = "Location"
	locs = self.get(n)
	ilocs = []
	self.maxLens = {}
	for l in locs["results"]:
	    # skip if loc is not for a gene we have seen
	    if not l[5] in self.gids:
	        continue
	    # keep track of the max end coord for each chromosome
	    self.maxLens[l[1]] = max(l[3], self.maxLens.get(l[1],0))
	    #
	    self.ofd.write(self.ITMPLTS[n] % {
		"id" : self.mkRef(n, l[0]),
		"locatedOn" : self.mkRef("Chromosome", l[1]),
		"start" : l[2],
		"end" : l[3],
		"strand" : l[4],
		"feature" : self.mkRef("Gene", l[5]),
	        })

    def __getitem__(self, n):
        return self.__dict__[n]

    def dump(self):
	self.ofd = open(self.file, "w")
	self.ofd.write('<?xml version="1.0"?>\n')
	self.ofd.write('<items>\n')
	#
	self.dumpDataSource()
	self.dumpDataSet()
	self.dumpOrganism()
	# order is important! first genes, then locations, then chromosomes
	self.dumpGenes()
	self.dumpLocations()
	self.dumpChromosomes()
	#
	self.ofd.write('</items>\n')
	self.ofd.close()

    #
    QTMPLTS = {
	"Organism" :  '''<query name="" model="genomic" view="Organism.id Organism.taxonId Organism.shortName" longDescription="" sortOrder="Organism.taxonId asc"> <constraint path="Organism.taxonId" op="=" value="%d"/></query>''',

	# This query follows a rather circuitous path to compensate for variations in how different mines
	# encode things. (E.g., in wormmine, chromosome's organism key is not set.) This query works in all
	# the mines.
	"Chromosome" : '''<query name="" model="genomic" view="Gene.chromosomeLocation.locatedOn.id Gene.chromosomeLocation.locatedOn.primaryIdentifier Gene.chromosomeLocation.locatedOn.name Gene.chromosomeLocation.locatedOn.length Gene.organism.id" longDescription="" sortOrder="Gene.chromosomeLocation.locatedOn.primaryIdentifier asc"> <constraint path="Gene.chromosomeLocation.locatedOn" type="Chromosome"/> <constraint path="Gene.organism.taxonId" op="=" value="%d"/> </query>''',

	"Gene" : '''<query name="" model="genomic" view="Gene.id Gene.primaryIdentifier Gene.symbol Gene.name Gene.organism.id Gene.chromosomeLocation.id Gene.sequenceOntologyTerm.identifier Gene.sequenceOntologyTerm.id" longDescription="" sortOrder="Gene.primaryIdentifier asc" constraintLogic="A and B"> <join path="Gene.chromosomeLocation" style="OUTER"/> <constraint path="Gene.organism.taxonId" code="A" op="=" value="%d"/> <constraint path="Gene.primaryIdentifier" code="B" op="IS NOT NULL"/> </query>''',

	"Location" : '''<query name="" model="genomic" view="Gene.chromosomeLocation.id Gene.chromosomeLocation.locatedOn.id Gene.chromosomeLocation.start Gene.chromosomeLocation.end Gene.chromosomeLocation.strand Gene.id" longDescription="" constraintLogic="A and B"> <constraint path="Gene.organism.taxonId" code="A" op="=" value="%d"/> <constraint path="Gene.primaryIdentifier" code="B" op="IS NOT NULL"/> </query>''',

        }

    #
    ITMPLTS = {
	"DataSource" : '''
<item class="DataSource" id="%(id)s">
    <attribute name="name" value="%(name)s" />
    <attribute name="url" value="%(url)s" />
    <attribute name="description" value="%(description)s" />
</item>
	''',

	"DataSet" : '''
<item class="DataSet" id="%(id)s">
    <attribute name="name" value="%(name)s" />
    <attribute name="version" value="%(version)s" />
    <attribute name="description" value="%(description)s" />
    <reference name="dataSource" ref_id="%(dataSource)s" />
</item>
''',

	"Organism" : '''
<item class="Organism" id="%(id)s" >
  <attribute name="taxonId" value="%(taxon)s" />
  </item>
''',
	"Chromosome" : '''
<item class="Chromosome" id="%(id)s">
  <attribute name="primaryIdentifier" value="%(primaryIdentifier)s" />
  %(length)s
  <reference name="organism" ref_id="%(organism)s" />
  </item>
''',
	"Gene" : '''
<item class="Gene" id="%(id)s">
  <attribute name="primaryIdentifier" value="%(primaryIdentifier)s" />
  <attribute name="symbol" value="%(symbol)s" />
  %(name)s
  <reference name="organism" ref_id="%(organism)s" />
  <reference name="sequenceOntologyTerm" ref_id="%(sequenceOntologyTerm)s" />
  %(chromosomeLocation)s
  <collection name="dataSets"><reference ref_id="%(dataSet)s" /></collection>
  </item>
''',
	"Location" : '''
<item class="Location" id="%(id)s">
  <reference name="feature" ref_id="%(feature)s" />
  <reference name="locatedOn" ref_id="%(locatedOn)s" />
  <attribute name="start" value="%(start)s" />
  <attribute name="end" value="%(end)s" />
  <attribute name="strand" value="%(strand)s" />
  </item>
''',
	"SOTerm" : '''
<item class="SOTerm" id="%(id)s" >
  <attribute name="identifier" value="%(identifier)s" />
  </item>
''',
        }

    #
    TYPEKEYS = {
	"Organism"	: 1,
	"Chromosome"	: 2,
	"Gene"		: 3,
	"Location"	: 4,
	"SOTerm"	: 5,
	"DataSource"	: 6,
	"DataSet"	: 7,
        }

##################################

def setUpCommandParser():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-D", "--debug", dest="debug", action="store_true", default=False,
	      help="Debug mode.")
    parser.add_option("-d", "--directory", dest="dir", action="store", type="string", default=".",
	      help="Where to write the output." )
    parser.add_option("-o", "--organism", dest="organism", action="store", type="string", default=None,
	      help="The species to dump data for. One of: "+str(MINES.keys()))
    return parser

def main():
    parser = setUpCommandParser()
    (opts,args) =  parser.parse_args(sys.argv)
    global LIMIT
    LIMIT = opts.debug and "&size=50" or ""

    if not opts.organism:
	parser.error("No organism specified.")
    cfg = MINES.get(opts.organism, None)
    if not cfg:
        parser.error("Organism not recognized. "+str(MINES.keys()))

    dir = os.path.abspath(opts.dir)
    if not os.path.isdir(dir):
        parser.error("Invalid directory path.")
    cfg['file'] = os.path.join(dir, "%s.features.xml"%opts.organism)
    d = FriendlyMineFeatureDumper(**cfg)
    d.dump()

if __name__ == "__main__":
    main()

