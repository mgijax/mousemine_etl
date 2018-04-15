#
# prepMgpGffFile.py
#
#    python prepMgpGffFile.py -s STRAIN 
#
# Performs specific file preprocessing for loading the MGP strain GFF files.
# - adds strain name to column 9
# - adds IDs to exons and UTRs
# - removes feature of type biological_region and chromosome
# - corrects type (column 3) errors:
#   * NMD_transcript_variant should be NMD_transcript
#   * RNA at gene level should be ncRNA_gene
# Reports all distinct types found in column 3.

import sys
import argparse
import re
#
EXCLUDE_TYPES = ["biological_region", "supercontig", "chromosome"]

#
TAB	= '\t'
NL	= '\n'
SEMI	= ';'
EQ	= '='
HASH	= '#'
BANG	= '!'
COMMA	= ','
#
SEQID	= 0
SOURCE	= 1
TYPE	= 2
START	= 3
END	= 4
SCORE	= 5
STRAND	= 6
PHASE	= 7
ATTRIBUTES = 8
#
GFF3HEADER = "##gff-version 3"
GFF3SEPARATOR = "###"
#
MGI_RE = re.compile(r'MGI:[0-9]+')
SEQACC_RE = re.compile(r'[A-Z][A-Z][0-9]+(\.[0-9])?')
#
class IdGenerator:
    def __init__(self):
        self.counts = {}

    def __call__(self, tp):
        val = self.counts.setdefault(tp, 1)
	self.counts[tp] += 1
	return '%s_%d' % (tp, val)
#
def log (s):
    sys.stderr.write(s+NL)

#
class MgpGffPrep:
    #
    def __init__(self):
	#
	self.idGen = IdGenerator()
	self.fin = sys.stdin
	self.fout= sys.stdout
	self.gffHeaderData = {}
	self.chromosomeData = {}
	self.validIds = set()
	self.idMapping = {}
	#
	self.args = self.parseArgs()

    #-------------------------------------------------
    def processMgiId(self, mgiid):
	v = None
	if mgiid in self.validIds:
	    v = mgiid
        else:
	    v = self.idMapping.get(mgiid, None)
	if v != mgiid:
	    if v is None:
		log("Removed invalid id: " + mgiid)
	    else:
		log("Mapped id %s to %s." % (mgiid,v))
	return v

    #-------------------------------------------------
    # Do all the munging needed for one GFF feature. 
    # This is the heart of the file preparation step.
    # FIXME: totally hard coded at this point. Configify!
    # Args:
    #    f - the feature
    # Returns:
    #    f, suitably munged, or None.
    #    None indicates that f should be omitted from the output.
    def processFeature(self, f):
	attrs = f[ATTRIBUTES]
	ident = attrs.get("ID", None)
	# if type is in exclude list, skip it
	if f[TYPE] in EXCLUDE_TYPES:
	    return None
	# correct specific errors found in use of SO terms
	# NMD_transcript_variant -> NMD_transcript
	if f[TYPE] == 'NMD_transcript_variant':
	    f[TYPE] = 'NMD_transcript'
	    log("Converted: NMD_transcript_variant: " + ident)
	# use of RNA as top level feature -> ncRNA_gene
	if f[TYPE] == 'RNA' and 'Parent' not in f[ATTRIBUTES]:
	    f[TYPE] = 'ncRNA_gene'
	    log("Converted: RNA as top level feature: " + ident)
	# promote protein coding genes
	if f[TYPE] == 'gene' and attrs.get('biotype','') == 'protein_coding':
	    f[TYPE] = 'protein_coding_gene'
	#
	# make sure exons and UTRs have IDs
	if 'ID' not in attrs:
	    if f[TYPE] == 'exon' and 'exon_id' in attrs:
		attrs['ID'] = attrs['exon_id']
	    else:
		attrs['ID'] = self.idGen(f[TYPE])
	else:
	    # strip leading prefix from ID
	    attrs['ID'] = self.stripPrefix(attrs['ID'])
	# strip leading prefix from Parent ID, if any
	if 'Parent' in attrs:
	    attrs['Parent'] = self.stripPrefix(attrs['Parent'])
	# add strain to every feature's col 9
	attrs['strain'] = self.args.strain
	# append strain to chromosome
	f[SEQID] = "%s|%s" % (f[SEQID], self.args.strain)
	# extract MGI id if it exists
	match = MGI_RE.search(attrs.get('description',''))
	if match:
	    # Replace description attribute with mgi_id.
	    attrs.pop('description',None)
	    mgiid = self.processMgiId(match.group(0))
	    if mgiid:
		attrs['mgi_id'] = mgiid
	# Avoid setting the symbol attribute in loaded features...
	n = attrs.pop('Name', None)
	if n:
	    attrs['mgp_name'] = n
	#
        return f

    #-------------------------------------------------
    def getFileContents(self, fname):
	fd = open(fname, 'r')
	s = fd.read()
	fd.close()
	return s

    #-------------------------------------------------
    def getFileLines(self, fname):
        s = self.getFileContents(fname)
	lines = s.split(NL)
	if lines[-1] == "":
	    del lines[-1]
        return lines

    #-------------------------------------------------
    def loadIdFiles (self) :
        if self.args.validfile:
	    self.validIds = set(self.getFileLines(self.args.validfile))
	    log("Loaded %d valid ids" % len(self.validIds))

	if self.args.mappingfile:
	    idpairs = map(lambda line: line.strip().split(TAB), self.getFileLines(self.args.mappingfile))
	    self.idMapping = dict(idpairs) 
	    log("Loaded %d id mappings" % len(self.idMapping))

    #-------------------------------------------------
    #
    def stripPrefix(self, s):
	pts = s.split(":", 1)
	if len(pts) == 1:
	    return s
	if pts[0] in ["gene", "transcript", "CDS"]:
	    return pts[1]
        return s

    #-------------------------------------------------
    #
    def parseArgs(self):
        self.parser = argparse.ArgumentParser(description='Prepare one GFF3 file from MGP.')
	self.parser.add_argument(
	    '-s',
	    '--strain',
	    metavar='strain',
	    help='Strain name')
	#self.parser.add_argument('-x', '--exclude', metavar='sotype', default=[], action='append', help='Col 3 types to exclude.')
	self.parser.add_argument(
	    '-v',
	    '--validFile',
	    dest="validfile",
	    metavar='FILE', 
	    default=None,
	    help='File of valid MGI primary ids. Default=no id file. All encountered MGI ids considered valid.')
	self.parser.add_argument(
	    '-m',
	    '--mappingFile',
	    dest="mappingfile",
	    metavar='FILE', 
	    default=None,
	    help='File of MGI primary and secondary ids. Two columns, tab delimited. Columns=primaryId, secondaryId. Default=no mapping file')

	self.parser.add_argument('-D',
	    '--debug',
	    dest="debug",
	    default=False,
	    action='store_true',
	    help='Debug.')

	return self.parser.parse_args()
    #
    def parseCol9(self, s):
	parts = dict([ part.split(EQ,1) for part in s.split(SEMI)])
	return parts
    #
    def parseLine(self, line):
	f = line.split(TAB)
	f[8] = self.parseCol9(f[8])
	return f
    #
    def formatCol9(self, attrs):
        s = SEMI.join(['%s=%s'%(k,v) for (k,v) in attrs.items()])
	return s
    #
    def formatLine(self, f):
        f2 = list(f)
	f2[8] = self.formatCol9(f2[8])
	return TAB.join([str(x) for x in f2])
    #
    def processCommentLine(self, line):
	if line == GFF3SEPARATOR or line == GFF3HEADER:
	    self.fout.write(line+NL)
	if line.startswith(HASH+HASH):
	    return
	if line.startswith(HASH+BANG):
	    parts = line[2:].strip().split()
	    self.gffHeaderData[parts[0]] = parts[1]
	self.fout.write(line+NL)
    #
    def processFeatureLine(self, line):
	f = self.processFeature(self.parseLine(line))
	if f:
	    self.fout.write(self.formatLine(f)+NL)
    #
    def main(self):
	self.loadIdFiles()
	for line in self.fin:
	    line = line[:-1]
	    if line.startswith(HASH):
		self.processCommentLine(line)
	    else:
		if self.args.debug:
		    print self.args
		    print self.gffHeaderData
		    sys.exit(1)
	        self.processFeatureLine(line)

####

if __name__ == "__main__":
    MgpGffPrep().main()

####
