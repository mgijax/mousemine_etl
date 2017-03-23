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
	self.tk2stanza = {}

    def formatSynonym(self, synonym, type="exact"):
	tag = type+"_synonym" if type else "synonym"
	synonym = '"%s" []' % synonym.strip()
        return (tag, synonym)

    def loadOmimFromMgi(self):
        # Load all OMIM terms from MGD.                                                                                    
        # Where: accid -> OMIM id and term -> OMIM name
        query = '''                                                                                                         
	  SELECT t._term_key, t.term, a.accid
	  FROM VOC_Term t, ACC_Accession a
	  WHERE t._vocab_key = 44
          AND a._object_key = t._term_key
          AND a._mgitype_key = 13
          AND a._logicaldb_key = 15
          AND a.preferred = 1
	'''

        for r in db.sqliter(query):
	    if not r['accid'].startswith('OMIM:'):
	        r['accid'] = 'OMIM:' + r['accid']
            omim_id = r['accid']
            omim_name_parts = r['term'].split(";")
            omim_name = omim_name_parts[0].strip()
	    omim_lines = [('id', omim_id), ('name', omim_name)]
	    if len(omim_name_parts) > 1:
		omim_lines.append( self.formatSynonym(omim_name_parts[1]) )
	    stanza = ('Term', omim_lines)
            self.stanzas.append( stanza )
	    self.tk2stanza[r['_term_key']] = stanza

	# Load secondary ids
	query = '''
	  SELECT a._object_key as _term_key, a.accid
	  FROM ACC_Accession a
	  WHERE a._mgitype_key = 13
          AND a._logicaldb_key = 15
          AND a.preferred = 0
	  ORDER BY a._object_key
	'''
	for r in db.sqliter(query):
	    if not r['accid'].startswith('OMIM:'):
	        r['accid'] = 'OMIM:' + r['accid']
	    tk = r['_term_key']
	    id = r['accid']
	    stanza = self.tk2stanza.get(tk, None)
	    if stanza:
		stanza[1].append( ('alt_id', id) )

	# Load synonyms
	query = '''
	  SELECT t._term_key, ms.synonym, ms._synonymtype_key
	  FROM MGI_Synonym ms, VOC_Term t
	  WHERE t._vocab_key = 44
	  AND t._term_key = ms._object_key
	  ORDER BY t._term_key
	'''
	for r in db.sqliter(query):
	    tk = r['_term_key']
	    stk = r['_synonymtype_key']
	    st = "exact" if stk==1017 else "broad"
	    s  = self.formatSynonym(r['synonym'], st)
	    stanza = self.tk2stanza.get(tk, None)
	    if stanza:
		stanza[1].append( s )


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
