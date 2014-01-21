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

LIMIT = ""

MINES = {
    10090: { 
	"name" : "MouseMine",
        "url" : "http://www.mousemine.org/mousemine", 
	"taxon" : 10090,
    },
    7227 : { 
	"name" : "FlyMine",
        "url" : "http://www.flymine.org/release-38.0", 
	"taxon" : 7227,
    },
    10116 : { 
	"name" : "RatMine",
        "url" : "http://ratmine.mcw.edu/ratmine", 
	"taxon" : 10116,
    },
    4932 : { 
	"name" : "YeastMine",
        "url" : "http://yeastmine.yeastgenome.org/yeastmine", 
	"taxon" : 4932,
    },
    7955 : { 
	"name" : "ZebraFishMine",
        "url" : "http://zmine.zfin.org", 
	"taxon" : 7955,
    },
    6239 : { 
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

    def iql(self, q):
	url = self.url+"/service/query/results?format=json&start=0"+ \
	      LIMIT+"&query="+urllib.quote(q)
	fd = urllib.urlopen(url)
	o=json.load(fd)
        fd.close()
	return o

    def mkRef(self, type, id):
        return "%d_%d" % (self.TYPEKEYS[type], id)

    def get(self, name):
        q = self.QTMPLTS.get(name, None)
	if not q: return None
	return self.iql(q%self.taxon)

    def dumpOrganism(self):
	n = "Organism"
        r = self.get(n)["results"][0]
	obj = { 
	  "id" : self.mkRef(n, r[0]),
	  "taxon" : r[1],
	  }
	org = self.ITMPLTS[n] % obj
	return org

    def dumpChromosomes(self):
        n = "Chromosome"
	chrs = self.get(n)
	ichrs = []
	for c in chrs["results"]:
	    obj = {
		"id" : self.mkRef(n, c[0]),
		"primaryIdentifier" : c[1] and c[1] or c[2],
		"organism" : self.mkRef("Organism", c[3]),
	        }
	    ichrs.append(self.ITMPLTS[n] % obj)
	return ichrs

    def dumpGenes(self):
        n = "Gene"
	genes = self.get(n)
	igenes = []
	sos = {}
	isoterms = []
	for g in genes["results"]:
	    sos[g[6]] = g[7]
	    cloc = g[5] and ('<reference name="chromosomeLocation" ref_id="%s" />'%self.mkRef("Location", g[5])) or ''
	    obj = {
		"id" : self.mkRef(n, g[0]),
		"primaryIdentifier" : g[1],
		"symbol" : g[2],
		"name" : g[3] and g[3].encode('ascii','ignore') or "",
		"organism" : self.mkRef("Organism", g[4]),
		"chromosomeLocation" : cloc,
		"sequenceOntologyTerm" : self.mkRef("SOTerm", g[7]),
	        }
	    igenes.append(self.ITMPLTS[n] % obj)
	for (sid, id) in sos.items():
	    isoterms.append( self.ITMPLTS["SOTerm"] % {
	        "id" : self.mkRef("SOTerm", id),
		"identifier" : sid,
		})
	return igenes + isoterms

    def dumpLocations(self):
        n = "Location"
	locs = self.get(n)
	ilocs = []
	for l in locs["results"]:
	    obj = {
		"id" : self.mkRef(n, l[0]),
		"locatedOn" : self.mkRef("Chromosome", l[1]),
		"start" : l[2],
		"end" : l[3],
		"strand" : l[4],
		"feature" : self.mkRef("Gene", l[5]),
	        }
	    ilocs.append(self.ITMPLTS[n]%obj)
	return ilocs

    def __getitem__(self, n):
        return self.__dict__[n]

    def dump(self):
	ofd = open(self.file, "w")
	ofd.write('<?xml version="1.0"?>\n')
	ofd.write(self.HEADERTMPLT%self)
	ofd.write('<items>\n')
	ofd.write( self.dumpOrganism())
	for o in self.dumpChromosomes():
	    ofd.write( o)
	for g in self.dumpGenes():
	    ofd.write( g)
	for l in self.dumpLocations():
	    ofd.write( l)
	ofd.write('</items>\n')
	ofd.close()

    #
    HEADERTMPLT = '''
    <!--
	Date: %(date)s
	Mine: %(name)s
	URL:  %(url)s
    -->
'''
    #
    QTMPLTS = {
	"Organism" :  '''<query name="" model="genomic" view="Organism.id Organism.taxonId Organism.shortName" longDescription="" sortOrder="Organism.taxonId asc"> <constraint path="Organism.taxonId" op="=" value="%d"/></query>''',

	# This query follows a rather circuitous path to componsate for variations in how different mines
	# encode things. (E.g., in wormmine, chromosome's organism key is not set.) This query works in all
	# the mines.
	"Chromosome" : '''<query name="" model="genomic" view="Gene.chromosomeLocation.locatedOn.id Gene.chromosomeLocation.locatedOn.primaryIdentifier Gene.chromosomeLocation.locatedOn.name Gene.organism.id" longDescription="" sortOrder="Gene.chromosomeLocation.locatedOn.primaryIdentifier asc"> <constraint path="Gene.chromosomeLocation.locatedOn" type="Chromosome"/> <constraint path="Gene.organism.taxonId" op="=" value="%d"/> </query>''',

	"Gene" : '''<query name="" model="genomic" view="Gene.id Gene.primaryIdentifier Gene.symbol Gene.name Gene.organism.id Gene.chromosomeLocation.id Gene.sequenceOntologyTerm.identifier Gene.sequenceOntologyTerm.id" longDescription="" sortOrder="Gene.primaryIdentifier asc" constraintLogic="A and B and C"> <join path="Gene.chromosomeLocation" style="OUTER"/> <constraint path="Gene.organism.taxonId" code="A" op="=" value="%d"/> <constraint path="Gene.primaryIdentifier" code="B" op="IS NOT NULL"/> <constraint path="Gene.symbol" code="C" op="IS NOT NULL"/> </query>''',

#	"Gene" : '''<query name="" model="genomic" view="Gene.id Gene.primaryIdentifier Gene.symbol Gene.name Gene.organism.id Gene.chromosomeLocation.id Gene.sequenceOntologyTerm.identifier Gene.sequenceOntologyTerm.id" longDescription="" sortOrder="Gene.primaryIdentifier asc"> <constraint path="Gene.organism.taxonId" op="=" value="%d"/><join path="Gene.chromosomeLocation" style="OUTER" /></query>''',

	"Location" : '''<query name="" model="genomic" view="Gene.chromosomeLocation.id Gene.chromosomeLocation.locatedOn.id Gene.chromosomeLocation.start Gene.chromosomeLocation.end Gene.chromosomeLocation.strand Gene.id" longDescription="" sortOrder="Gene.chromosomeLocation.locatedOn.primaryIdentifier asc"> <constraint path="Gene.organism.taxonId" op="=" value="%d"/> </query>''',

        }

    #
    ITMPLTS = {
	"Organism" : '''
<item class="Organism" id="%(id)s" >
  <attribute name="taxonId" value="%(taxon)s" />
  </item>
''',
	"Chromosome" : '''
<item class="Chromosome" id="%(id)s">
  <attribute name="primaryIdentifier" value="%(primaryIdentifier)s" />
  <reference name="organism" ref_id="%(organism)s" />
  </item>
''',
	"Gene" : '''
<item class="Gene" id="%(id)s">
  <attribute name="primaryIdentifier" value="%(primaryIdentifier)s" />
  <attribute name="symbol" value="%(symbol)s" />
  <attribute name="name" value="%(name)s" />
  <reference name="organism" ref_id="%(organism)s" />
  <reference name="sequenceOntologyTerm" ref_id="%(sequenceOntologyTerm)s" />
  %(chromosomeLocation)s
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
        }

##################################

def setUpCommandParser():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option("-D", "--debug", dest="debug", action="store_true", default=False,
	      help="Debug mode.")
    parser.add_option("-d", "--directory", dest="dir", action="store", type="string", default=".",
	      help="Where to write the output." )
    parser.add_option("-t", "--taxon", dest="taxon", action="store", type="int", default=None,
	      help="NCBI Taxonomy ID of the species to dump data for. One of: "+str(MINES.keys()), metavar="TAXONID")
    return parser

def main():
    parser = setUpCommandParser()
    (opts,args) =  parser.parse_args(sys.argv)
    global LIMIT
    LIMIT = opts.debug and "&size=50" or ""

    if not opts.taxon:
	parser.error("No taxon specified.")
    cfg = MINES.get(opts.taxon, None)
    if not cfg:
        parser.error("Taxon not recognized. "+str(MINES.keys()))

    dir = os.path.abspath(opts.dir)
    if not os.path.isdir(dir):
        parser.error("Invalid directory path.")
    cfg['file'] = os.path.join(dir, "%d.features.xml"%opts.taxon)
    d = FriendlyMineFeatureDumper(**cfg)
    d.dump()

if __name__ == "__main__":
    main()

