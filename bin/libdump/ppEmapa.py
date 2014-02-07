import sys
import re
from OboParser import *

def processStanza(stype, slines):
    if stype == 'Typedef':
	print formatStanza(stype, slines)
        return
    olines = []
    for sl in slines:
	if sl[0] == "id" and not sl[1].startswith("EMAPA:"):
	    return
	if sl[0] == "relationship":
	    tokens = sl[1].split()
	    #print tokens, tokens[0] in ["starts_at", "ends_at"]
	    if tokens[0] in ["starts_at", "ends_at"]:
		olines.append( (tokens[0], int(tokens[1][2:]) ))
		continue
	olines.append(sl)
    print formatStanza(stype, olines)

OboParser(processStanza).parseFile(sys.stdin)
