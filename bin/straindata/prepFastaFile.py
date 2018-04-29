#
# prepFastaFile.py
#
# (1) Splits FASTA input into separate files per chromosome.
# (2) Modifies descriptor lines so that the chromosome identifier
# looks like "<chr>|<strainname>", eg, "6|AKR/2J".
#
# Usage:
#    python prepFastaFile.py -s <strainname> -o <outputdir> -f <inputfilename>
#
import sys
import argparse

class FastaPrepper:
    def __init__(self) :
        self.args = self.getArgs()
	self.chr2ofile = {}
	self.ifd = None
	self.ofd = None
	self.log("prepFastaFile: args=" + str(self.args))
	#
	#print self.args
	#print "Sample output file:", self.getOfileName("14")
	#sys.exit(-1)

    def log(self, msg):
        sys.stderr.write(msg + '\n')

    def getArgs(self):
	parser = argparse.ArgumentParser(description='Process a whole genome FASTA file by splitting into one file/chromosome, removing scaffold sequences, munging the chromosome label.')
	#
	parser.add_argument('-s', '--strain', dest='strainName', help='The strain name / print label, eg, "C57BL/6J". Required.')
	parser.add_argument('-l', '--localName', dest='localName', help='The thing to use in file names, eg c57bl6j. Optional. Default=based on strainName: all lowercase, slashed removed.')
	parser.add_argument('-i', '--inputFile', dest='ifile', default='-', help='The input file. For stdin, use "-". Default == stdin.')
	parser.add_argument('-o', '--outputDirectory', dest='odir', default='.', help='The output directory.')
	#
	args = parser.parse_args()
	#
	if not args.strainName:
	    parser.error('A strain is required. Please specify -s.')
	if not args.localName:
	    args.localName = args.strainName.replace('/', '').lower()
	#
	return args

    def openInput(self):
        if self.args.ifile == "-":
	    self.ifd = sys.stdin
	else:
	    self.ifd = open(self.args.ifile, 'r')

    def getOfileName(self, chrom):
	return "%s/%s.chromosome.%s.fa" % (self.args.odir, self.args.localName, chrom)

    def openOutput(self, chrom):
	if self.ofd:
	    self.ofd.close()
	ofname = self.getOfileName(chrom)
	self.ofd = open(ofname, 'w')
	self.log("prepFastaFile: writing to " + ofname)

    #
    # Example header:
    # >6 dna_rm:chromosome chromosome:GRCm38:6:1:149736546:1 REF
    #
    def processHeaderLine(self, line):
	if not "chromosome" in line:
	    if self.ofd:
	        self.ofd.close()
		self.ofd = None
	    return line
	bits = line.split()
	#
	chrom=bits[0][1:]	# strip off the '>'
	self.openOutput(chrom)
	#
	bits[0] = '>' + chrom + "|" + self.args.strainName
	return ' '.join(bits) + '\n'

    #
    def main(self) :
	self.openInput()
	for line in self.ifd:
	    if line.startswith(">"):
		line=self.processHeaderLine(line)
	    if self.ofd:
		self.ofd.write(line)

#
if __name__ == "__main__" :
    FastaPrepper().main()
