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
from libdump import mgidbconnect as db

##########################################
VERSION = "0.1"

##########################################
# The order of dumpers is important. The objects dumped later
# in the process may refer to objects dumped earlier.
# It is assumed that all ontologies have already been loaded.
# The following lists all dumper classes and their dependencies.
# To run any dumper, you have to first run the dumpers in
# its dependency list. This is recursive.
allDumpers = [
    (PublicationDumper,         []),
    (DataSourceDumper,          []),
    (OrganismDumper,            []),
    (ChromosomeDumper,          [OrganismDumper]),
    (StrainDumper,              [OrganismDumper,PublicationDumper]),
    (FeatureDumper,             [ChromosomeDumper,DataSourceDumper,PublicationDumper]),
    (ProteinDumper,             [FeatureDumper]),
    (LocationDumper,            [FeatureDumper]),
    (HomologyDumper,            [FeatureDumper]),
    (SyntenyDumper,             [FeatureDumper]),
    (AlleleDumper,              [FeatureDumper,StrainDumper]),
    (CellLineDumper,            [AlleleDumper]),
    (GenotypeDumper,            [CellLineDumper]),
    (ExpressionDumper,          [GenotypeDumper,FeatureDumper]),
    (HTIndexDumper,             [ExpressionDumper]),
    (AnnotationCommentDumper,   []),
    (AnnotationDumper,          [GenotypeDumper,AlleleDumper,FeatureDumper]),
    (RelationshipDumper,        [AlleleDumper,FeatureDumper]),
    (SynonymDumper,             [AlleleDumper]),
    (CrossReferenceDumper,      [AlleleDumper]),
    ]

# create map from each class to its dependencies
dependencies = dict(allDumpers)

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
            print(VERSION)
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
            cls=eval(v+"Dumper")
            clcs.append( cls )
    if len(clcs) == 0:
        clcs = map(lambda t: t[0], allDumpers)

    if checkRefs:
        # Expand class list to include dependencies.
        # Depth-first order
        final = []
        def _add(c):
            if c in final:
                return
            for dc in dependencies[c]:
                _add(dc)
            final.append(c)
        for c in clcs:
            _add(c)
        clcs = final

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
    db.setConnectionFromPropertiesFile()
    dcx.log("Database connection:" + str(db.getConnection()))
    #
    total = 0
    for cls in clcs:
        total += cls(dcx).dump(fname=cls.__name__[:-6]+".xml")
    #
    dcx.closeOutputs()
    dcx.log("Finished MGI item dump.")
    dcx.log("Grand total: %d items written."%total)
    dcx.log("============================================================")

##########################################
main(sys.argv[1:])
