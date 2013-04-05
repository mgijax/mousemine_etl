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


    def loadMedic(self, file):
        def stanzaProc(stype, slines):
            self.stanzas.append((stype, slines))
        OboParser(stanzaProc).parseFile(file)


    def loadOmimFromMedic(self, slines):
        for tag, val in slines:
            if (tag == "id" or tag == "alt_id") and val.startswith("OMIM"):
                self.omim_from_medic.add(val)

    
    def tupleInjectTag(self, t, tag):
        temp = list(t)
        temp[0] = tag
        return tuple(temp)
    

    def swapMeshWithOmim(self, slines):
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

        if swap:
            slines[mesh] = self.tupleInjectTag(slines[mesh], 'alt_id')
            slines[omim] = self.tupleInjectTag(slines[omim], 'id')


    def processMedic(self):
        for stype, slines in self.stanzas:
            self.loadOmimFromMedic(slines)
            self.swapMeshWithOmim(slines)


    def loadOmimFromMgi(self):
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
            self.id2name[oid] = r['term']
            self.omim_from_mgi.add(oid)


    def loadSynonymsFromMgi(self):
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
            self.omim_to_synonym[r['accid']].add(r['synonym'])


    def appendMgiOmim(self):
        # Adds omim terms at the root
        #   - if needed in the tree, include a 'is a' key-value pair
        missing_omim_ids = self.omim_from_mgi - self.omim_from_medic

        for oid in missing_omim_ids:
            omim_stanza = ('Term', [('id', oid), ('name', self.id2name[oid])])
            self.stanzas.append(omim_stanza)


    def stripIdPrefix(self, id):
        id_split = id.split(':', 1)
        return id_split[1]


    def conflateAltIds(self):
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
        for stype, slines in self.stanzas:
            for tag, val in slines:
                if tag == "id":
                    for synonym in self.omim_to_synonym[self.stripIdPrefix(val)]:
                        slines.append(('synonym', '"' + synonym + '" []'))


    def writeStanzas(self, file):
        fd = open(file, 'w')

        for stype, slines in self.stanzas:
            fd.write(formatStanza(stype, slines))
            fd.write('\n')

        fd.close()


    def main(self, medic_file, conflated_file):
        self.loadMedic(medic_file)
        self.processMedic()

        self.loadOmimFromMgi()
        self.loadSynonymsFromMgi()
        self.appendMgiOmim()

        self.conflateAltIds()
        self.appendMgiSynonyms()
        self.writeStanzas(conflated_file)


MedicConflater().main(sys.argv[1], sys.argv[2])
