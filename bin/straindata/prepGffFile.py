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
import gff3lite as gff3
#
TAB = '\t'
NL = '\n'
HASH = '#'
BANG = '!'
#
EXCLUDE_TYPES = [
  "biological_region",
  "supercontig",
  "chromosome",
  "match",
  "match_part",
  ]

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
	self.currentLine = None
	self.fout= sys.stdout
	self.gffHeaderData = {}
	self.chromosomeData = {}
	self.validIds = set()
	self.idMapping = {}
	#
	self.args = self.parseArgs()
	self.args.isMGI = (self.args.strain == 'C57BL/6J')

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
    # Process MGI feature group. Have to turn a model as output by the MGI GFF3 process
    # into a model as needed by the strain loaded.
    #
    def processMGIFeatureGroup(self, grp):
	#
	newgrp = []
	mgiid = None
	newid = None
	source = None
	# loop thru the feature in the groups
	for i,f in enumerate(grp):
	    #print ">>>", f
	    attrs = f[gff3.ATTRIBUTES]
	    newattrs = {}
	    fid = attrs['ID']
	    if i==0 :
		# grp[0] is the top level feature
		if f[gff3.TYPE] in EXCLUDE_TYPES:
		    return
	        mgiid = fid
		newattrs['mgi_id'] = mgiid
		# find the dbxref to use as ID
		newid = self.chooseId(f)
		if newid is None: 
		    continue
		source = newid.split(':',1)[0]
		newid = newid.split(':',1)[1]
		self.idMapping[mgiid] = newid
		newattrs['ID'] = newid
		f[gff3.SOURCE] = source
		f[gff3.TYPE] = attrs['so_term_name']
	    elif f[gff3.SOURCE] == source:
		tp = f[gff3.TYPE]
		p = attrs['Parent']
		pid = newattrs['Parent'] = self.idMapping.get(p,p)
		if tp == 'exon':
		    newattrs['ID'] = attrs.get('exon_id', fid.replace(mgiid, pid))
		elif tp == 'CDS':
		    newattrs['ID'] = attrs['protein_id']
		elif 'transcript_id' in attrs:
		    tid = attrs['transcript_id']
		    self.idMapping[fid] = tid
		    newattrs['ID'] = tid
	    else:
	        continue
	    #
	    # if type is in exclude list, skip it
	    if f[gff3.TYPE] in EXCLUDE_TYPES:
	        continue
	    # add strain to every feature's col 9
	    newattrs['strain'] = self.args.strain
	    # append strain to chromosome
	    f[gff3.SEQID] = "%s|%s" % (f[gff3.SEQID], self.args.strain)
	    #
	    f[gff3.ATTRIBUTES] = newattrs
	    newgrp.append(f)
	#
	for f in newgrp:
	    self.fout.write(gff3.formatLine(f) + NL)
	if len(newgrp) > 0:
	    self.fout.write(gff3.GFF3SEPARATOR + NL)

    #-------------------------------------------------
    def chooseId(self, f):
        dbxrs = {}
	dbxr = f[gff3.ATTRIBUTES].get('Dbxref',None)
	if dbxr is None:
	    return None
	for dbx in dbxr.split(','):
	    prefix,ident = dbx.strip().split(":",1)
	    dbxrs[prefix] = ident
	pref = ['ENSEMBL','miRBase','NCBI_Gene']
	for p in pref:
	    ident = dbxrs.get(p, None)
	    if ident: return p+':'+ident
	return None

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
	attrs = f[gff3.ATTRIBUTES]
	ident = attrs.get("ID", None)
	# if type is in exclude list, skip it
	if f[gff3.TYPE] in EXCLUDE_TYPES:
	    return None
	# correct specific errors found in use of SO terms
	# NMD_transcript_variant -> NMD_transcript
	if f[gff3.TYPE] == 'NMD_transcript_variant':
	    f[gff3.TYPE] = 'NMD_transcript'
	    log("Converted: NMD_transcript_variant: " + ident)
	# use of RNA as top level feature -> ncRNA_gene
	if f[gff3.TYPE] == 'RNA' and 'Parent' not in f[gff3.ATTRIBUTES]:
	    f[gff3.TYPE] = 'ncRNA_gene'
	    log("Converted: RNA as top level feature: " + ident)
	# promote protein coding genes
	if f[gff3.TYPE] == 'gene' and attrs.get('biotype','') == 'protein_coding':
	    f[gff3.TYPE] = 'protein_coding_gene'
	#
	# make sure exons and UTRs have IDs
	if 'ID' not in attrs:
	    if f[gff3.TYPE] == 'exon' and 'exon_id' in attrs:
		attrs['ID'] = attrs['exon_id']
	    else:
		attrs['ID'] = self.idGen(f[gff3.TYPE])
	else:
	    # strip leading prefix from ID
	    attrs['ID'] = self.stripPrefix(attrs['ID'])
	# strip leading prefix from Parent ID, if any
	if 'Parent' in attrs:
	    attrs['Parent'] = self.stripPrefix(attrs['Parent'])
	# add strain to every feature's col 9
	attrs['strain'] = self.args.strain
	# append strain to chromosome
	f[gff3.SEQID] = "%s|%s" % (f[gff3.SEQID], self.args.strain)
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

	return self.parser.parse_args()
    #
    def processHeader(self, header):
	#print '>>>', header
	for i, line in enumerate(header):
	    if i > 0 and line.startswith(HASH+HASH):
		continue
	    if line.startswith(HASH+BANG):
		parts = line[2:].strip().split()
		self.gffHeaderData[parts[0]] = parts[1]
	    self.fout.write(line+NL)
    #
    def processFeatureGroup(self, grp):
	for feat in  filter(None, map(lambda f: self.processFeature(f), grp)):
	    self.fout.write(gff3.formatLine(feat) + NL)
	self.fout.write(gff3.GFF3SEPARATOR + NL)
    #
    def main(self):
	self.loadIdFiles()
	it = gff3.iterate(self.fin)
	header = it.next()
	self.processHeader(header)
	for grp in it:
	    if self.args.isMGI:
		self.processMGIFeatureGroup(grp)
	    else:
		self.processFeatureGroup(grp)

####

if __name__ == "__main__":
    MgpGffPrep().main()

####
