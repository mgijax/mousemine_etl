#                                                                                                                           
# OmimDumper.py                                                                                                         
#                                                                                                                           
# Usage:                                                                                                                    
#   python OmimDumper.py <output_file>                                                                     
#                                                                                                                           
# Writes an obo formatted file of OMIM terms from MGD                                                                                              
#                                                                                                                           


from OboParser import OboParser, formatStanza
import mgiadhoc as db
import os
import sys

class OmimDumper:

    def __init__(self):
        self.omim_from_mgi = []
        self.stanzas = []


    def loadOmimFromMgi(self):
        # Load all OMIM terms from MGD.                                                                                    
        # Where: accid -> OMIM id and term -> OMIM name
        query = '''                                                                                                         
                SELECT t.term, a.accid                                                                                      
                FROM VOC_Term t, ACC_Accession a                                                                            
                WHERE t._vocab_key = 44                                                                                     
                   AND a._object_key = t._term_key                                                                          
                   AND a._mgitype_key = 13                                                                                  
                   AND a._logicaldb_key = 15                                                                                
                '''

        for r in db.sql(query):
            omim_id = "OMIM:" + r['accid']
            omim_name_parts = r['term'].split(";")
            omim_name = omim_name_parts[0].strip()
            omim_tuple = (omim_id, omim_name)
            
            self.omim_from_mgi.append(omim_tuple)

    def createOmimStanzas(self):
        for (omim_id, omim_name) in self.omim_from_mgi:
            omim_stanza = ('Term', [('id', omim_id), ('name', omim_name)])
            self.stanzas.append(omim_stanza)

    def writeStanzas(self, file):
        # write out the stanza in obo format                                                                                
        fd = open(file, 'w')

        for stype, slines in self.stanzas:
            fd.write(formatStanza(stype, slines))
            fd.write('\n')

        fd.close()


    def main(self, omim_output_file):
        self.loadOmimFromMgi()
        self.createOmimStanzas()
        self.writeStanzas(omim_output_file)


db.setConnectionDefaultsFromPropertiesFile()
OmimDumper().main(sys.argv[1])
