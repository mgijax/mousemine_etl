#
# prepMgpGffFile.py
#
#    python prepMgpGffFile.py -s STRAIN -v VALID_ID_FILE -m ID_MAP_FILE < INPUT > OUPUT
#
# Performs specific file preprocessing for loading the strain-specific GFF files.
# - adds strain name to column 9
# - adds IDs to exons and UTRs
# - removes features of type biological_region and chromosome
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
  ]

#
MGI_RE = re.compile(r'MGI:[0-9]+')
SEQACC_RE = re.compile(r'[A-Z][A-Z][0-9]+(\.[0-9])?')
#
class IdGenerator:
    def __init__(self):
        self.counts = {}

    def __call__(self, prefix):
        val = self.counts.setdefault(prefix, 1)
	self.counts[prefix] += 1
	return '%s_%d' % (prefix, val)
#
def log (s):
    sys.stderr.write(s+NL)

#
def partition(lst, f):
    d = {}
    for elt in lst:
        val = f(elt)
	d.setdefault(val,[]).append(elt)
    return d
#
class GffPrep:
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
    # into a file as needed by the gff3 loader for mousemine.
    # Loader expects:
    #   - mgi_id attribute in col 9 of root features contains MGI id of canonical gene (if any)
    #   - a valid SO type in col 3 => becomes the class of the loaded object
    #
    def processMGIFeatureGroup(self, grp):
	#
	newgrp = []
	mgiid = None
	newid = None
	source = None
	# loop thru the features in the group
	for i,f in enumerate(grp):
	    #print ">>>", f
	    #
	    # if type is in exclude list, skip it
	    if f[gff3.TYPE] in EXCLUDE_TYPES:
	        continue
	    #
	    attrs = f[gff3.ATTRIBUTES]
	    newattrs = {}
	    fid = attrs['ID']
	    if i==0 :
		# grp[0] is the top level feature
	        mgiid = fid
		newattrs['mgi_id'] = mgiid
		newid = mgiid.replace('MGI:', 'MGI_C57BL6J_')
		self.idMapping[mgiid] = newid
		newattrs['ID'] = newid
		f[gff3.SOURCE] = 'MGI'
		f[gff3.TYPE] = attrs['so_term_name']
	    else:
		tp = f[gff3.TYPE]
		p = attrs['Parent']
		pid = newattrs['Parent'] = self.idMapping.get(p,p)
		if tp == 'exon':
		    newattrs['ID'] = attrs.get('exon_id', fid.replace(mgiid, pid))
		elif tp == 'CDS':
		    newattrs['ID'] = attrs.get('protein_id', fid)
		elif 'transcript_id' in attrs:
		    tid = attrs['transcript_id']
		    self.idMapping[fid] = tid
		    newattrs['ID'] = tid
		elif fid:
		    newattrs['ID'] = fid
		else:
		    newattrs['ID'] = self.makeId(f)

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
    def makeId(self, f):
	tp = f[gff3.TYPE].split('_')[-1]  # use an abbreviated type
	strain = self.args.strain.replace('/', '') # prepend strain name
	prefix = "%s_%s" % (strain,tp)
	return self.idGen(prefix)

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
	# make sure exons, UTRs, etc have IDs
	if 'ID' not in attrs:
	    if f[gff3.TYPE] == 'exon' and 'exon_id' in attrs:
		attrs['ID'] = attrs['exon_id']
	    else:
		attrs['ID'] = self.makeId(f)
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
	# prep each record and remove any Nones
	grp = filter(None, map(lambda f: self.processFeature(f), grp))
	# partition into exons and non-exons
	pp = partition(grp, lambda f: f[gff3.TYPE] == "exon")
	# write out the non-exons first
	for feat in pp.get(False,[]):
	    self.fout.write(gff3.formatLine(feat) + NL)
	#
	# reduce the exons. MGP GFF3 files have the following quirk: a given exon appears once for 
	# in which it is included. Each occurrence is on a separate line. They all have the same coordinates,
	# and ID. The only diff is the Parent, which is the particular transcript ID for that occurrence.
	# Official GFF3 spec sez there should be a single exon feature, with a comma separated list of 
	# transcript IDs in the Parent field. Ie, what we get from MGP:
	#
	# 1  MGP  exon  10 20 . + . ID=exon1;Parent=transcript1
	# 1  MGP  exon  10 20 . + . ID=exon1;Parent=transcript2
	# 1  MGP  exon  10 20 . + . ID=exon1;Parent=transcript3
	#
	# What we want instead:
	#
	# 1  MGP  exon  10 20 . + . ID=exon1;Parent=transcript1,transcript2,transcript3
	#
	exons = {}
	eidOrder = []
	for feat in pp.get(True,[]):
	    fid = feat[gff3.ATTRIBUTES]['ID']
	    if fid not in exons:
		exons[fid] = feat
		eidOrder.append(fid)
	    else:
	        f2 = exons[fid]
		f2[gff3.ATTRIBUTES]['Parent'] += ("," + feat[gff3.ATTRIBUTES]['Parent'])
	#
	for eid in eidOrder:
	    feat = exons[eid]
	    self.fout.write(gff3.formatLine(feat) + NL)
	#
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
    GffPrep().main()

####
