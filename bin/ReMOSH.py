#
# ReMOSH.py
#
# Takes CTD's version of the MOSH (the one with more 
# merges) + the spreadsheet containing OMIM-to-MESH 
# mappings and outputs MGI's version of the MOSH (the
# one with more leaves).
#
# Usage:
#   python ReMOSH.py CTDMOSHFILE SPREADSHEETFILE > MGIMOSHFILE
#
# Both MOSH files are in .obo format.
#
# The spreadsheet file is tab-delimited text. Every line represents
# the mapping of one OMIM term to one MESH term. Every OMIM term
# (that we use) is represented in this file; only MESH terms with
# mapped OMIM ids are represented. The same OMIM id may appear on 
# more that 1 line, e.g., as a leaf under several MESH terms.
# Wrinkle #1. Multi-merges. (FIXME) There is a whole class of OMIM terms (those
# corresponding to OMIIM gene records) that may be merged (i.e., marked
# as 'M') with multiple MESH terms. Plays havoc with the concept of an id.
# Really not sure how best to handle this, but for now, the
# script makes the OMIM term a child of all the MESH terms (basically,
# switches the M calls to L's). 
#
# The spreadsheet has 12 columns
# (although be careful - whenever I download the file from
# Google docs, certain lines mysteriously have only 11 columns.)
# Here we only care about the first seven columns:
#    0. MESH term 
#    1. MESH id
#    2. CTD's action call (L or M)
#    3. MGI's action call (L or M)
#    4. OMIM term
#    5. OMIM id
#    6. OMIM type code
# 
# The script reads the spreadsheet file, building mappings in memory.
# It then makes a filtering/transformation pass over the input MOSH file:
# OMIM ids/terms are stripped from the input, essentially leaving the MESH
# "skeleton". Then the OMIM stuff is re-added based on the spreadsheet,
# using MGI's action calls. (The input should reflect the CTD action
# calls in the spreadsheet, but this is not checked.) 
#
# In more detail: The MOSH contains two types of nodes: (1) a MESH term
# with zero or more OMIM ids attached (as alt_id's), (2) or an OMIM
# term with one or more MESH term parents (as is_a's).
# For type 1 nodes, we strip off all the OMIM alt_id's, then re-add them
# based on the MGI action calls in the spreadsheet.
# Type 2 nodes are filtered out of the input. Then new type 2 nodes are
# added for all spreadsheet lines where MGI calls the OMIM id an "L".
#
# Wrinkle: The spreadsheet contains MESH and OMIM terms as well as IDs.
# However, the terms are unreliable (not updated) and should not be used.
# The input MOSH contains all the names EXCEPT for all those OMIM terms
# that were merges but became leaves. 
#
# Consistency checks (produce log messages): 
# Sets of interest:
#   MI: MeSH IDs in the input file
#   MS: MeSH IDs in the spreadsheet file
#   OS: OMIM IDs in the spreadsheet file
#   OM: OMIM IDs/terms from MGI
# 
# All MeSH IDs in the ss should exist in the input: MS subset of MI
# All OMIM terms in MGI are mapped, and all mapped OMIM IDs have terms: OS == OM
#

import sys
import re
from libdump.OboParser import OboParser, formatStanza
import mgiadhoc as db
import logging

# keys from MGD
OMIM_VOCABKEY	= 44	# from VOC_Vocab
OMIM_LDBKEY	= 15	# from ACC_LogicalDB
TERM_TYPEKEY	= 13	# from ACC_MGIType

NL	= "\n"
TAB	= "\t"
SP	= " "
EMPTY	= ""

class ReMosher(object):
    def __init__(self):
	self.oid2mappings = {}	# mapping from OMIM id to spreadsheet lines for that id
        self.mid2altids = {}	# mapping from Mesh id to merged OMIM ids (from spreadsheet)
	self.altids = set()	# set of OMIM ids that are merged into a MESH term
	self.outfd = sys.stdout
	self.logfd = sys.stderr
	self.id2name = {}	# mapping from OMIM id->name (loaded from MGI)
	self.stanzas = []	# MOSH stanzas are accumulated first, then written
				# (To detect OMIM ids merged to multiple MESH terms,
				# have to read the whole thing.)
	self.leafifiedCount = 0
	self.MI = set()	# MESH ids in the input OBO file
	self.MS = set()	# MESH ids in the spreadsheet
	self.OS = set()	# OMIM ids in the spreadsheet
	self.OM = set()	# OMIM ids from MGI

    def log(self, s):
        self.logfd.write(s)
        self.logfd.write('\n')

    def write(self, s):
        self.outfd.write(s)

    def writeStanza(self, stype, slines):
        self.write(formatStanza(stype,slines))
	self.write(NL)

    def loadOmimFromMgi(self):
	# loads OMIM ids and terms from MGI. This is just to be sure we
	# use exacly the same OMIM names as MGI (don't rely on spreadsheet).
        q = '''
	    SELECT 
	      vt.term,
	      aa.accid
	    FROM 
	      VOC_Term vt, 
	      ACC_Accession aa
	    WHERE vt._vocab_key = %d
	    AND vt._term_key = aa._object_key
	    AND aa._mgitype_key = %d
	    and aa._logicaldb_key = %d
	    AND aa.preferred = 1
	    AND aa.private = 0
	    '''%(OMIM_VOCABKEY,TERM_TYPEKEY,OMIM_LDBKEY)
	for r in db.sql(q):
	    oid = "OMIM:"+r['accid']
	    self.id2name["OMIM:"+r['accid']] = r['term']
	    self.OM.add(oid)

	q = '''
	    SELECT p.accid as primaryid, s.accid as secondaryid
	    FROM ACC_Accession p, ACC_Accession s
	    WHERE p.preferred = 1 AND p.private = 0
	    AND s.preferred = 0 AND s.private = 0
	    AND p._object_key = s._object_key
	    AND p._mgitype_key = s._mgitype_key
	    AND p._logicaldb_key = s._logicaldb_key
	    AND p._logicaldb_key = %d
	    ''' % OMIM_LDBKEY
	self.secondary2primary = {}
	for r in db.sql(q):
	    self.secondary2primary["OMIM:"+r['secondaryid']] = "OMIM:"+r['primaryid']

    def readSpreadsheetFile(self,file):
	# Loads the CTD/MGI spreadsheet file that maps OMIM ids to MeSH terms.
	# mname	= [0]	MeSH term
	# mid	= [1]	MeSH id
	# ctd	= [2]	CTD's action (L or M)
	# mgi	= [3]	MGI's action (L or M)
	# oname	= [4]	OMIM term
	# oid	= [5]	OMIM id
	# otype	= [6]	OMIM type code
	#----
	def processLine(line):
	    tokens = line.split(TAB)
	    if len(tokens) < 7 or not tokens[5].isdigit():
		return
	    tokens[1] = "MESH:"+(tokens[1].strip())
	    tokens[5] = "OMIM:"+(tokens[5].strip())
	    self.oid2mappings.setdefault(tokens[5],[]).append(tokens)
	    self.MS.add(tokens[1])
	    self.OS.add(tokens[5])
	#----
	fd = open(file,'r')
	for line in fd:
	    processLine(line)
	fd.close()
	#----
	# Here is where we look for omim ids that merge to multiple
	# mesh terms, and change them to be leaves instead.
        for oid, mappings in self.oid2mappings.iteritems():
	    leafified = False
	    for m in mappings:
		if m[3] == 'M':
		    if len(mappings) > 1:
			leafified = True
			m[3] = 'L'
		    else:
			# 
			self.mid2altids.setdefault(m[1], []).append(m[5])
			self.altids.add(m[5])
	    if leafified:
		self.leafifiedCount += 1
		self.log("\nMULTIPLE MERGE NOT SUPPORTED: %s leaf-ified\n%s\n"%(oid, str(mappings)))

    def processStanza(self, stype, slines):
	if stype != 'Term':
	    self.stanzas.append(( stype, slines ))
	    return
	id = slines[0][1].strip()
	name = slines[1][1]	# FIXME. ASSUMES position

	# Here we map every id in the input to its name
	self.id2name.setdefault(id,name)

	if id.startswith("MESH:"):
	    self.MI.add(id)
	    # Term is a Mesh term
	    slines = filter(lambda x:not (x[0]=="alt_id" and x[1].startswith("OMIM")), slines)
            slines = filter(lambda x:x[0] != "synonym", slines)
	    for m in self.mid2altids.get(id,[]):
	        if m[3] == "M":
		    slines.append( ("alt_id", m) )
		else:
		    self.oid2mappings.setdefault(m[5], []).append(m)
	    self.stanzas.append((stype, slines))
	else:
	    # Term is an OMIM term
	    pass

    def postProcess(self):
	def report(idset, message):
	    if len(idset) > 0:
		idlist = list(idset)
		idlist.sort()
		for id in idlist:
		    self.log(message%id)
		self.log("Count: %d" % len(idset))

	# All MeSH IDs in the ss should exist in the input: MS subset of MI
	report( (self.MS-self.MI), "MESH id from spreadsheet not found in input: %s")
	report( (self.OM-self.OS), "OMIM id from MGI has no mapping in spreadsheet: %s")
	report( (self.OS-self.OM), "OMIM id from spreadsheet not found in MGI: %s")

	# write out all the MESH terms (including merged OMIM ids)
	for stype, slines in self.stanzas:
	    self.writeStanza(stype,slines)

	# now write out all the leaf OMIM terms
	for oid, ms in self.oid2mappings.iteritems():
	    if oid in self.altids:
	        continue
	    slines = [
		('id' , oid),
		('name', self.id2name.get(oid, ms[0][4]))
	        ]
	    for m in ms:
		slines.append( ('is_a', "%s ! %s"%(m[1],self.id2name.get(m[1],m[0]))))
	    self.writeStanza('Term', slines)


    def main( self, obofile, ssfile ):
	self.loadOmimFromMgi()
	self.readSpreadsheetFile(ssfile)
	OboParser(self.processStanza).parseFile(obofile)
	self.postProcess()
	self.log("\nLeafified OMIM ids=%d\n"%self.leafifiedCount)

ReMosher().main(sys.argv[1], sys.argv[2])

