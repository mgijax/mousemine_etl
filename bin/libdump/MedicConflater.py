#
# MedicConflater.py
#
# Usage:
#   python MedicConflater.py <input_file> <output_file>
#
# Both files are in .obo format
#
# The script reads a medic file, adds useful information to the stanzas, and writes stanzas out.
#

from OboParser import OboParser, formatStanza
from collections import defaultdict
import mgiadhoc as db
import os
import sys

class MedicConflater:
    
    def __init__(self):
        self.omim_from_medic = set()
        self.omim_from_mgi = set()
        self.omim_to_synonym = defaultdict(set)
        self.id2name = {}
        self.stanzas = []
	self.swappedIds = {}


    def loadMedic(self, file):
	# reads/parses the medic .obo file. Result is a list of stanzas.
        def stanzaProc(stype, slines):
	    # Looks for OMIM ids in a stanza. Adds them to a set,
            for i, line in enumerate(slines):
		tag, val = line
		if stype is None and tag == "default-namespace":
		    slines[i] = (tag, "MEDIC_disease_ontology")
                if (tag == "id" or tag == "alt_id") and val.startswith("OMIM"):
                    self.omim_from_medic.add(val)
            self.stanzas.append((stype, slines))
        OboParser(stanzaProc).parseFile(file)

    def loadOmimFromMgi(self):
	# Loads all OMIM terms from MGI.
	# Creates index from omim id -> term.
	# Also maintains a set of term ids.
        query = '''
                SELECT t.term, a.accid
                FROM VOC_Term t, ACC_Accession a
                WHERE t._vocab_key = 44
                   AND a._object_key = t._term_key
                   AND a._mgitype_key = 13
                   AND a._logicaldb_key = 15
                '''

        for r in db.sql(query):
            oid = "OMIM:" + r['accid']
	    nparts = r['term'].split(";")
	    for i in range(1, len(nparts)):
	        self.omim_to_synonym[r['accid']].add(nparts[i].strip())
            self.id2name[oid] = nparts[0].strip()
            self.omim_from_mgi.add(oid)


	# Loads synonyms for disease (OMIM) terms from MGI.
	# Creates index from omim id -> synonym(s).
        query = '''
                SELECT t.term, a._object_key, s.synonym, a.accid
                FROM VOC_Term t, ACC_Accession a, MGI_Synonym s
                WHERE t._vocab_key = 44
                   AND a._object_key = t._term_key
                   AND s._object_key = a._object_key
                   AND a._mgitype_key = 13
                   AND a._logicaldb_key = 15
                '''

        for r in db.sql(query):
	    nparts = r['synonym'].split(';')
	    for n in nparts:
	        self.omim_to_synonym[r['accid']].add(n.strip())

    def stripIdPrefix(self, id):
	# strips leading prefix (up to ":") from the id and returns the rest.
        id_split = id.split(':', 1)
        return id_split[1]

    def swapMeshWithOmim(self):
	# If the given stanza has a MESH primary id and an OMIM alt_id,
	# swap them, i.e., make the OMIM id primary and the MESH alt_id.
	for stype, slines in self.stanzas:
            mesh = None
            omim = None
            swap = False
            
            for index, sline in enumerate(slines):
                (tag, val) = sline
                if tag == 'id' and val.startswith("MESH"):
                    mesh = index
                    swap = omim is not None
                elif tag == 'alt_id' and val.startswith("OMIM"):
                    if omim is None:
                        omim = index
                        swap = mesh is not None
                    else:
                        #more than one omim alt_id
                        swap = False
                        break
	    # end for

            if swap:
                slines[mesh] = ('alt_id', slines[mesh][1])
                slines[omim] = ('id', slines[omim][1])
	        self.swappedIds[slines[mesh][1]] = slines[omim][1]
	# end for

    def appendMgiOmim(self):
        # Adds a stanza for each OMIM id found in MGI but not in MEDIC.
	# These all become independent roots. (Or, if you prefer, orphan nodes.)
        missing_omim_ids = self.omim_from_mgi - self.omim_from_medic

        for oid in missing_omim_ids:
            omim_stanza = ('Term', [('id', oid), ('name', self.id2name[oid])])
            self.stanzas.append(omim_stanza)

    def addAltIds(self):
	# adds the un-prefixed versions of Mesh and Omim ids as alt_ids.
	# e.g., for MESH:D654342 and OMIM:12345, adds "D65432" and "12345" as alt_ids
        for stype, slines in self.stanzas:
            ids = set()
            alt_ids = set()

            for tag, val in slines:
                if tag == "id" and (val.startswith("MESH") or val.startswith("OMIM")):
                    ids.add(self.stripIdPrefix(val))

                if (tag == "alt_id"):
                    if ':' in val:
                        ids.add(self.stripIdPrefix(val))
                    else:
                        alt_ids.add(val)

            for id in ids:
                if not id in alt_ids:
                    slines.append(('alt_id', id))


    def appendMgiSynonyms(self):
	# adds all MGI synonyms
        for stype, slines in self.stanzas:
            for tag, val in slines:
                if tag in ["id","alt_id"]:
                    for synonym in self.omim_to_synonym[self.stripIdPrefix(val)]:
                        slines.append(('synonym', '"' + synonym + '" []'))

    def replaceDiseaseName(self):
	# replaces the disease name with the one from MGI
        for stype, slines in self.stanzas:
	    id = None
	    j = None
            for i,line in enumerate(slines):
		tag, val = line
		if tag == "id":
		    id = val
                elif tag == "name":
		    j = i
	    if j:
		origName = slines[j][1]
		newName = self.id2name.get(id, origName )
	        slines[j] = (slines[j][0], newName)
		if origName != newName \
		and not newName.lower().startswith(origName.lower()):
		    self.omim_to_synonym[self.stripIdPrefix(id)].add(origName)
		    

    def swapIsaIds(self):
	# converts "is_a:" lines when the target MESH id has been replaced with the OMIM
        for stype, slines in self.stanzas:
	    for (i,line) in enumerate(slines):
                tag, val = line
                if tag == "is_a":
		    meshid, rest = val.split(None,1)
		    omimid = self.swappedIds.get(meshid,None)
		    if omimid:
			slines[i] = (tag, "%s %s" % (omimid, rest))


    def writeStanzas(self, file):
	# write out the stanza in obo format
        fd = open(file, 'w')

        for stype, slines in self.stanzas:
            fd.write(formatStanza(stype, slines))
            fd.write('\n')

        fd.close()


    def main(self, medic_file, conflated_file):
        self.loadMedic(medic_file)
        self.loadOmimFromMgi()

        self.appendMgiOmim()

        self.swapMeshWithOmim()
	self.replaceDiseaseName()
        self.appendMgiSynonyms()
        self.addAltIds()
	self.swapIsaIds()

        self.writeStanzas(conflated_file)


MedicConflater().main(sys.argv[1], sys.argv[2])
