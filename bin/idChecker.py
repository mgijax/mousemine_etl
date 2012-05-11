#
# idChecker.py
#
# A simple script to verify id integrity in a set of ItemXML files.
# This means:
#	1. Item ids are unique
#	2. There are no dangling references.
#
# Inputs to the script may comprise any combination of file names and/or directory names;
# directories are expanded to the list of its files (nonrecursive, single level). 
# If no inputs are specified, the script reads from standard input.
# The universe of objects to be checked is defined by the union of the contents of all the inputs.
# 
# If no problems are detected, the script produces no output and exits with a
# status of 0. Otherwise, it prints errors to stdout, and exits with a non-zero status.
#
# NOTE: for a given id, this script reports ONLY THE FIRST occurrence of an error.
#

import sys
import os
import re

id_re = re.compile(r'(ref_)?id *= *"([^"]+)"')
errors = False
idx = {}
refidx = {}

def log(m):
    sys.stderr.write(m)
    sys.stderr.flush()

def readFile( ifd ):
    global errors, idx, refidx
    lineNum = 0
    log("Reading from file: %s\n"%ifd.name)
    for line in ifd:
	lineNum += 1
	val = (ifd.name, lineNum)
	for m in id_re.finditer(line):
	    id = m.group(2)
	    if m.group(1):
		# This is a cross-ref to the id. If we've already seen the id, nothing to do.
		# If we haven't, record the xref for later.
		if id not in idx:
		    refidx.setdefault(id, val)
	    else:
		# This is an id. First make sure it's not a duplicate. 
		if id in idx:
		    errors = True
		    print "Duplicate reference: id=%s file=%s line=%d" %(id, val[0], val[1])
		else:
		    # New id. Record it. Also, if there's a pending xref, remove it
		    idx[id] = val
		    refidx.pop(id, None)

def checkRefs( ):
    # Any remaining xrefs are dangling.
    global errors, idx, refidx
    log("Checking xrefs\n")
    items = refidx.items()
    items.sort(None, lambda i:(i[1][0],i[1][1]))
    for (refid, val) in items:
	errors = True
	print "Dangling reference: id=%s file=%s line=%d" %(refid, val[0], val[1])

def main():
    idx = {}
    refidx = {}

    files = []
    for a in sys.argv[1:]:
        if os.path.isdir(a):
	    for f in os.listdir(a):
	        files.append(os.path.abspath(os.path.join(a, f)))
	else:
	    files.append(a)
    
    if len(files) == 0:
	readFile(sys.stdin)
    else:
        for f in files:
	    fd = open(f,'r')
	    readFile(fd)
	    fd.close()

    checkRefs( )
    if errors:
        sys.exit(-1)
    else:
        sys.exit(0)

main()
