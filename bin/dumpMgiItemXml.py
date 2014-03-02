#!/usr/bin/python
# 
# dumpMgiItemXml.py
#
# Dump MGI data into Intermine ItemXML format.
#
##########################################

import sys
import getopt
from libdump import *
import types
import os
from libdump import mgiadhoc as db

##########################################
VERSION = "0.1"

##########################################
# The order of dumpers is important. The objects dumped later
# in the process may refer to objects dumped earlier.
# It is assumed that all ontologies have already been loaded.
allDumpers = [
    (PublicationDumper,		()),
    (DataSourceDumper,		()),
    (OrganismDumper,		()),
    (ChromosomeDumper,		()),
    (StrainDumper,		()),
    (FeatureDumper,		()),
    (LocationDumper,		()),
    (SyntenyDumper,		()),
    (AlleleDumper,		()),
    (CellLineDumper,		()),
    (GenotypeDumper,		()),
    (ExpressionDumper,		()),
    (AnnotationDumper,		()),
    (SynonymDumper,		()),
    (CrossReferenceDumper,	()),
    ]

defaultParams = dict(allDumpers)

##########################################
##########################################
def parseArgs(argv):
    opts,args = getopt.getopt(argv, 
        'c:d:D:l:vL:p:', 
	['class=', 'dir=','define','debug', 'limit=','version','logfile=','norefcheck','install=','properties='])
    return opts,args

def main(argv):
    opts,args=parseArgs(argv)
    debug=False
    dir='.'
    limit = None
    clcs = []
    defs = {}
    logfile=None
    pfile = "~/.intermine/mousemine.properties"
    checkRefs = True
    for o,v in opts:
        if o == '--debug':
	    debug=True
	elif o in ('-D','--define'):
	    n,val = v.split('=',1)
	    defs[n]=val
	elif o in ('-d', '--dir'):
	    dir = v
	elif o in ('-l','--limit'):
	    limit = int(v)
	elif o in ('-v', '--version'):
	    print VERSION
	    sys.exit(0)
	elif o == '--norefcheck':
	    checkRefs = False
	elif o in ('-L','--logfile'):
	    logfile = v
	elif o in ('-p','--properties'):
	    pfile = v
	elif o == '--install':
	    m = __import__(v)
	    installMethods(m)
	elif o in ('-c', '--class'):
	    i=v.find("(")
	    if i == -1:
		cls=eval(v+"Dumper")
		args=defaultParams[cls]
	    else:
		cls=eval(v[0:i]+"Dumper")
		args=eval(v[i:])
		if type(args) is not types.TupleType:
		    args= (args,)
	    clcs.append( (cls,args) )
    if len(clcs) == 0:
        clcs = allDumpers[:]
	        

    dcx = DumperContext(
    	debug=debug, 
	dir=dir, 
	limit=limit, 
	defs = defs, 
	logfile=logfile, 
	checkRefs=checkRefs)
    dcx.log("\n============================================================")
    dcx.log("Starting MGI item dump...")
    dcx.log("Command line parameters = %s" % str(argv))

    db.setConnectionDefaultsFromPropertiesFile(pfile)
    dcx.log("Database connection:" + str(db.getConnectionDefaults()))
    total = 0
    for cls,args in clcs:
	total += cls(dcx, *args).dump(fname=cls.__name__[:-6]+".xml")
    dcx.closeOutputs()
    dcx.log("Finished MGI item dump.")
    dcx.log("Grand total: %d items written."%total)
    dcx.log("============================================================")

##########################################
main(sys.argv[1:])
