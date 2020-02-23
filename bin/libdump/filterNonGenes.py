#
# filterNonGenes.py
#
# Removes lines from Homologene or Panther files that refer to non-gene
# mouse markers. Reads from stdin, writes to stdout. 
# Extended to also filter BioGrid and IntAct files.
#
# USAGE:
#       $ python filterNonGenes.py -t [homologene|panther|biogrid|intact] < INPUT > OUTPUT
#
# Why:
# 
# Homologene, Panther, et. al. contain entries for non-gene mouse
# features such as gene segments and pseudogenes. Often this is legitimate data,
# however, the current load software does not handle them well. Since it
# only looks at Genes, it doesn't find them in the db. So it goes ahead
# and creates new Gene objects with duplicate ids.
#
#

import os
import sys
import logging
import xml.etree.ElementTree as et
import mgidbconnect as db


TAB = '\t'
PIPE = '|'
NL = '\n'
suppressed = set()

def cacheGeneIds():
    geneIds = set()
    query = '''
        SELECT aa.accid
        FROM ACC_Accession aa, MRK_MCV_Cache mm
        WHERE  aa._mgitype_key = 2
        AND aa.private = 0
        AND aa._object_key = mm._marker_key
        AND mm.term = 'gene'
        AND aa._logicaldb_key in (1,13,59,60,85,131,132,133,134)
        ''' 
    db.sql(query, lambda r: geneIds.add(r['accid']))
    return geneIds

#
# A filter reads an input, filters out certain data, and writes the rest to output.
# The output has that same format as the input. Abstract superclass.
# 
class Filter:
    def __init__(self, ifd, ofd, validIds):
        self.ifd = ifd
        self.ofd = ofd
        self.validIds = validIds

#
# Abstract superclass for simple, line-oriented formats (e.g. tsv).
# Each subclass define a test() method that takes a line and returns True
# if the line should be written out and False if not.
#
class LineByLineFilter(Filter):
    def go(self):
        for line in self.ifd:
            if self.test(line):
                self.ofd.write(line)

# Homologene: tab delimited file, one line per gene, 
class HomologeneFilter(LineByLineFilter):
    def test(self, line):
        fields = line.split(TAB)
        if fields[1] == '10090':
            # write mouse line only if it has a valid id
            id = fields[2]
            return id in self.validIds
        else:
            # Write out non mouse lines
            return True

# Panther: tab delimited file of pairs. Two genes per line, in columns 1 and 2.
class PantherFilter(LineByLineFilter):
    def _test1(self, field):
        # Only filtering mouse
        if not field.startswith('MOUSE'):
            return True
        # Panther mostly uses MGI ids for mouse, but some line use Ensembl id.
        id = None
        if field.startswith('MOUSE|MGI'):
            id = field.split(PIPE)[1].replace("MGI=MGI=", "MGI:")
        elif field.startswith('MOUSE|Ensembl'):
            id = field.split(PIPE)[1].replace("Ensembl=","")
        return id in self.validIds

    def test(self, line):
        # have to test both fields. Both must pass for the line to pass.
        fields = line.split(TAB)
        return self._test1(fields[0]) and self._test1(fields[1])


#
# BioGrid and IntAct data are in XML format and are highly structured.
# The overall tag structure (summarized/condensed):
# Indentation means nesting. Asterisks mean repeatible (an in a list). 
# Selected tag attributes of interest denoted by name= (e.g., id=)
#
# entrySet level= version= xmlns=
#  * entry
#      source
#        ...
#      experimentList
#        * experimentDescription id=
#            ...
#      interactorList
#        * interactor id=
#            xref
#              * secondaryRef db= id=
#            organism ncbiTaxId=
#            ...
#      interactionList
#         *interaction
#            experiment
#              ...
#            participantList
#               * participant 
#                   interactorRef
#                   ...
#
# The filter removes: (1) every interactor where the organism is mouse but is not
# matched to an MGI gene, and (2) every interaction having a participant removed by (1).
#
# The implementation uses xml.etree.iterparse() to process the file. 
#
# Pieces of the parsed element tree are discarded as soon possible, so the memory 
# footprint remains small even for very large files.
#

class BioGridFilter(Filter):
    def __init__(self, ifd, ofd, validIds):
        self.ifd = ifd
        self.ofd = ofd

        self.HEAD = '<?xml version="1.0" encoding="UTF-8"?>\n'
        self.TAIL = ''

        self.validIds = validIds
        self.removedInteractors = set()

    def formatAttrib(self, attrib):
        attrib = self.reformatXmlNs(attrib)
        return " ".join(['%s="%s"'%(k,v) for k,v in list(attrib.items())])

    def reformatXmlNs(self, attrib):
        xmlxsiurl='http://www.w3.org/2001/XMLSchema-instance'
        xmlxsipname = "{%s}schemaLocation"%xmlxsiurl
        xmlxsival = attrib.pop(xmlxsipname,None)
        if xmlxsival is None:
            return attrib
        attrib['xmlns'] = xmlxsival.split()[0]
        attrib['xmlns:xsi'] = xmlxsiurl
        attrib['xsi:schemaLocation'] = xmlxsival
        return attrib

    def printTagStart(self, tag, elt):
        self.ofd.write('<'+tag+' ' + self.formatAttrib(elt.attrib) + '>\n')

    def printTagEnd(self, tag):
        self.ofd.write('</'+tag+'>\n')

    def filterInteractor(self, elt):
        o = elt.find("organism")
        if o is None:
            # Chemicals.
            return True
        if o.attrib["ncbiTaxId"] != "10090":
            # don't filter non-mouse
            return False
        # allow mouse only if we can verify that it is a gene
        for sr in elt.findall('.//primaryRef')+elt.findall('.//secondaryRef'):
            if sr.attrib['db'] == "mgd/mgi":
                mgiid = 'MGI:' + sr.attrib['id']
                if mgiid in self.validIds:
                    return False
                else:
                    self.removedInteractors.add(elt.attrib['id'])
                    return True
            elif sr.attrib['db'][0:7] in ("ensembl","uniprot") and sr.attrib['id'] in self.validIds:
                return False
        self.removedInteractors.add(elt.attrib['id'])
        return True

    def filterInteraction(self, elt):
        # filter the interaction if any participant was filtered.
        for ir in elt.findall('.//interactorRef'):
            if ir.text in self.removedInteractors:
                return True
        return False
            
    ### 

    def defaultContainer(self, evt, tag, elt):
        if evt == "start":
            self.printTagStart(tag, elt)
        elif evt == "end":
            self.printTagEnd(tag)

    def defaultItem(self, evt, tag, elt):
        if evt == "end":
            self.ofd.write(et.tostring(elt).decode('utf-8'))
            self.ofd.write("\n")
            elt.clear()
            self.stack[-2].remove(elt)


    ###

    def entrySet(self, evt, tag, elt):
        self.defaultContainer(evt,tag,elt)

    def entry(self, evt, tag, elt):
        self.defaultContainer(evt,tag,elt)

    def source(self, evt, tag, elt):
        self.defaultItem(evt, tag, elt)

    def experimentList(self, evt, tag, elt):
        if self.stack[-2].attrib['shortTag'] == "entry":
            self.defaultContainer(evt,tag,elt)

    def experimentDescription(self, evt, tag, elt):
        self.defaultItem(evt, tag, elt)

    def interactorList(self, evt, tag, elt):
        self.defaultContainer(evt,tag,elt)

    def interactor(self, evt, tag, elt):
        if evt == "end" and not self.filterInteractor(elt):
            self.defaultItem(evt, tag, elt)

    def interactionList(self, evt, tag, elt):
        self.defaultContainer(evt,tag,elt)

    def interaction(self, evt, tag, elt):
        if evt == "end" and not self.filterInteraction(elt):
            self.defaultItem(evt, tag, elt)

    ###

    def go(self):
        self.ofd.write(self.HEAD)
        self.stack = []
        for evt, elt in et.iterparse(self.ifd,events=("start","end")):
            tag = elt.tag.split("}")[-1]
            if evt == "start":
                elt.attrib['shortTag'] = tag
                self.stack.append(elt)
            elif evt == "end":
                elt.tag = tag
            m = getattr(self,tag,None)
            if m:
                m(evt,tag,elt)
            if evt == "end":
                self.stack.pop()

        self.ofd.write(self.TAIL)


def getFilterClass(ftype):
    if ftype == "homologene":
        return HomologeneFilter
    elif ftype == "panther":
        return PantherFilter
    elif ftype == "biogrid" or ftype == "intact":
        return BioGridFilter
    else:
        raise RuntimeError("Unrecognized file type: " + ftype)

def getCmdLine():
    from optparse import OptionParser
    op = OptionParser()
    op.add_option("-t", "--type", dest="filetype", choices=["homologene","panther","biogrid","intact"],
                  help='File type. One of: "homologene","panther","biogrid","intact"')
    op.add_option("-o", "--output", dest="output", default=None,
                  help="Where to send the output. If not specified, or is '-', sends to stdout.")
    op.add_option("-u", "--updateInPlace", dest="inplace", default=False, action="store_true",
                  help="Update the input files in place.")
    (options, ifiles) = op.parse_args()
    if len(ifiles) == 0:
        ifiles = ["-"]
    if options.output and options.inplace:
        op.error("Please specify -o or -u, but not both.")
    if not options.output and not options.inplace:
        options.output = "-"
    if options.inplace and "-" in ifiles:
        op.error("Option -u incompatible with using stdin for input.")
    if options.filetype is None:
        op.error("No filetype specified.")
    return (options,ifiles)

def main():
    opts, ifiles = getCmdLine()
    geneIds = cacheGeneIds()
    if opts.output == "-":
        ofd = sys.stdout
    elif opts.output:
        ofd = open(opts.output, 'w')
    for ifile in ifiles:
        if ifile == "-":
            ifd = sys.stdin
        else:
            ifd = open(ifile, 'r')
            if opts.inplace:
                ofile = ifile + ".tmp"
                ofd = open(ofile, 'w')
        filter = getFilterClass(opts.filetype)(ifd, ofd, geneIds)
        filter.go()
        if opts.inplace:
            ofd.close()
            os.system("mv %s %s" % (ofile,ifile))

db.setConnectionFromPropertiesFile()
main()
