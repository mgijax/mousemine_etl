#
# filterDiseaseOntology.py
#
# USAGE:
#   $ python filterDiseaseOntology.py < INPUT > OUTPUT
#   
#   where INPUT is the downloaded doid-merged.obo file and OUTPUT is the corrected file
#
#
# WHAT IT DOES:
# Collects only DOID stanzas (ignoring obsolete stanzas) from the input obo file.
# For each DOID stanza:
#  - replaces all 'xref' tags with 'alt_id' tags
#  - for each omim id add a line with 'alt_id' tag and just the omim number
#
# This script reads from stdin and writes to stdout.
#
# Note: this script assumes the first entry in the stanza is the 'id' tag
#


from .OboParser import OboParser, formatStanza
import os
import sys

class FilterDiseaseOntology:

    def __init__(self):
        self.doid_header = []
        self.doid_stanzas = []

    def loadAndFilterDO(self):
        def stanzaProc(stype, slines):
            isDoidStanza = False
            isObsolete = False
            omimIds = []

            if stype is None:
                #save the header
                self.doid_stanzas.append((stype, slines))
            else:
                for i, line in enumerate(slines):
                    tag, val = line
                # only interested in DOID stanzas
                    if tag == "id" and val.startswith("DOID:"):
                        isDoidStanza = True
                    elif isDoidStanza and not isObsolete:
                        if val.startswith("OMIM:"):
                        # save all OMIM id numbers
                            omimIds.append(val.replace("OMIM:", ""))
                        if tag == "xref":
                        #replace all "xref" tags with "alt_id"
                            slines[i] = (tag.replace("xref", "alt_id"),val)
                        elif tag == "is_obsolete" and val == "true":
                        #ignore all obsolete stanzas
                            isObsolete = True

                if isDoidStanza and not isObsolete:
                    for omimId in omimIds:
                    # add additional lines of ("alt_id", <OMIM Id (only number)>
                        slines.append(("alt_id", omimId))
                    self.doid_stanzas.append((stype, slines))

        OboParser(stanzaProc).parseFile(sys.stdin)

    def writeStanzas(self):
        for stype, slines in self.doid_stanzas:
            sys.stdout.write(formatStanza(stype, slines))
            sys.stdout.write('\n')

    def main(self):
        self.loadAndFilterDO()
        self.writeStanzas()

FilterDiseaseOntology().main()
