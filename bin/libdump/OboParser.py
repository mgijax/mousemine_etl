#------------------------------------
#
# OboParser
#
# Rudimentary parser for OBO format files. Parses lines into groups
# called stanzas. Passes each stanza to a provided function that processes
# the stanza. A stanza is simply a dict mapping string keys to list-of-strings 
# values. 
#
# Example. Here's a stanza from an OBO file:
#
# [Term]
# id: GO:0000001
# name: mitochondrion inheritance
# namespace: biological_process
# def: "The distribution of ..." [GOC:mcc, PMID:10873824, PMID:11389764]
# exact_synonym: "mitochondrial inheritance" []
# is_a: GO:0048308 ! organelle inheritance
# is_a: GO:0048311 ! mitochondrion distribution
#
# Here's the dict that would be passed to the processing function:
#
# {
# "__type__" : [ "Term" ]
# "id" : [ "GO:0000001" ]
# "name" : [ "mitochondrion inheritance" ]
# "namespace" : [ "biological_process" ]
# "def" : [ '"The distribution ..." [GOC:mcc, PMID:10873824, PMID:11389764]' ]
# "exact_synonym" : [ '"mitochondrial inheritance" []' ]
# "is_a" : [ "GO:0048308 ! organelle inheritance", "GO:0048311 ! mitochondrion distribution" ]
# }
#
# Things to note about the stanza:
#    The stanza's type is passed under the pseudo key "__type__".
#    (The header stanza has no type.)
#    Lines in the file having the same key are combined into a list under that
#    key. E.g., the two "is_a" lines in the example.
#    ALL values in the dict are lists, even if only a single value is allowed by OBO.
#    The stanza dict is reused for each new stanza. Thus the user's processing
#    function must copy any needed information out of the stanza before returning.
#

import sys
import types

TYPE = "__type__"
LINES= "__lines__"
HEADER="__header__"
COMMENTCHAR = "!"
NL="\n"

class OboParser(object):
    def __init__(self, stanzaProcessor):
        self.fd = None
	self.stanzacount = None
	self.stanza = None
	self.stanzaProcessor = stanzaProcessor

    def __clearStanza__(self):
	self.stanza = { TYPE : None, LINES : [] }

    def parseFile(self, file):
	if type(file) is types.StringType:
	    self.fd = open(file, 'r')
	else:
	    self.fd = file
	self.__go__()
	if type(file) is types.StringType:
	    self.fd.close()

    def __finishStanza__(self):
	if len(self.stanza[LINES]) > 0:
	    self.stanzacount += 1
	    self.stanzaProcessor(self.stanza[TYPE],self.stanza[LINES])
	    self.__clearStanza__()

    def __parseLine__(self, line):
	if line.startswith("["):
	    j = line.find("]",1)
	    return (TYPE, line[1:j])
	else:
	    j = line.find(":")
	    return (line[0:j], line[j+1:].strip())

    def __addToStanza__(self, line):
	k,v = self.__parseLine__(line)
	if k==TYPE:
	    self.stanza[TYPE] = v
	else:
	    self.stanza[LINES].append( (k,v) )
        
    def __go__(self):
	self.__clearStanza__()
	self.stanzacount = 0
	for line in self.fd:
	    if line.startswith(COMMENTCHAR):
	        continue
	    elif len(line) == 1:
		self.__finishStanza__()
	    else:
	        self.__addToStanza__(line)
	self.__finishStanza__()

def formatStanza(stype, slines):
    t = stype and "[%s]\n"%stype or ""
    s = NL.join(map(lambda x: "%s: %s"%(x[0],x[1]), slines))+NL
    r = t+s
    return r

if __name__ == "__main__":
    def p(s,l):
        print formatStanza(s,l)
    OboParser(p).parseFile(sys.stdin)
