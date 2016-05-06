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
import time

TIMESTAMP = time.strftime("%m:%d:%Y %H:%M",time.localtime(time.time()))
HEADER = '''format-version: 1.2
date: %s
default-namespace: omim

''' % TIMESTAMP

class OmimDumper:

    def __init__(self):
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
            omim_stanza = ('Term', [('id', omim_id), ('name', omim_name)])
	    if len(omim_name_parts) > 1:
		omim_syn = '"%s" EXACT []' % omim_name_parts[1].strip()
		if len(omim_syn) > 0:
		    omim_stanza[1].append( ('synonym', omim_syn) )
            self.stanzas.append(omim_stanza)

    def writeStanzas(self, file):
        # write out the stanza in obo format                                                                                
        fd = open(file, 'w')
        fd.write(HEADER)

        for stype, slines in self.stanzas:
            fd.write(formatStanza(stype, slines))
            fd.write('\n')

        fd.close()


    def main(self, omim_output_file):
        self.loadOmimFromMgi()
        self.writeStanzas(omim_output_file)


db.setConnectionDefaultsFromPropertiesFile()
OmimDumper().main(sys.argv[1])
