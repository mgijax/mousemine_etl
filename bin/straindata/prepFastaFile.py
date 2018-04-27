#
# processFasta.py
#
# Applied while uncompressing. Modify descriptor lines so that the chromosome identifier
# looks like "<chr>|<strainname>", eg, "6|AKR/2J".
#
import sys

strain = sys.argv[1]

#
# >6 dna_rm:chromosome chromosome:GRCm38:6:1:149736546:1 REF
#
def processHeaderLine(line):
    if not "chromosome" in line:
        return None
    bits = line.split()
    bits[0] = bits[0] + "|" + strain
    return ' '.join(bits) + '\n'

skip=False
for line in sys.stdin:
    if line.startswith(">"):
	skip=False
        line=processHeaderLine(line)
	if not line:
	    skip=True
    if not skip:
	sys.stdout.write(line)
