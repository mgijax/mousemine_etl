#
# OmimAdder.py
#
# Usage:
#   python OmimAdder.py <input_file> <output_file>
#
# Both files are in .obo format
#
# The script writes the input file and any missing MGI OMIM ids from the input file.
#

from OboParser import OboParser, formatStanza
import mgiadhoc as db 
import os
import sys

class OmimAdder:

    def __init__(self):
        self.omim_from_medic = set()
        self.omim_from_mgi = set()
        self.missing_omim_from_medic = set()
        self.id2name = {}
        self.stanzas = []


    def loadOmimIdsFromMedic(self, file):
        def stanzaProc(stype, slines):
            self.stanzas.append( (stype, slines) )
            for tag,val in slines:
                if (tag == "id" or tag == "alt_id") and val.startswith("OMIM"):
                    self.omim_from_medic.add(val) 

        OboParser(stanzaProc).parseFile(file)


    def loadOmimIdsFromMgi(self):
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


    def appendStanzas(self, omim_ids):
        # Adds omim terms at the root
        #   - if needed in the tree, include a 'is a' key-value pair
        for oid in omim_ids:
            omim_stanza = ('Term', [('id', oid), ('name', self.id2name[oid])])
            self.stanzas.append(omim_stanza)


    def writeStanzas(self, file):
        fd = open(file, 'w')
        
        for stype, slines in self.stanzas:
            fd.write(formatStanza(stype, slines))
            fd.write('\n')

        fd.close()


    def main(self, obo_file, output_file):
        self.loadOmimIdsFromMedic(obo_file)
        self.loadOmimIdsFromMgi()

        missing_omim_ids = self.omim_from_mgi - self.omim_from_medic
        self.appendStanzas(missing_omim_ids)
        self.writeStanzas(output_file)
        
        
OmimAdder().main(sys.argv[1], sys.argv[2])
