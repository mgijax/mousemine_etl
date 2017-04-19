#
# CheckForMgiUpdate.py
#
# Check every properties file in ~/.intermine 
# If any one has a project.releaseVersion property that does not match the MGI
# exit with 'RUN_ETL_UPDATE=Update-ETL' to inject jenkins environment variable
# Otherwise exit with 'ETL data is up to date'.
#

import sys
import re
import os
from libdump import mgidbconnect as db

class VersionGetter:
    def __init__(self):
	self.fname = 'lastdump_date'
	self.query = 'select %s from mgi_dbinfo' % self.fname
	self.dtmplt= '%Y-%m-%d'
        db.setConnectionFromPropertiesFile()
	self.date = db.sql(self.query)[0][self.fname]
	self.versionString = 'MGI update: %s' % self.date.strftime(self.dtmplt)

class VersionChecker:
    def __init__(self):
	self.fname_re = r"\.properties"		# regex to match file names
	self.dir = "~/.intermine"		# directory to look in
	self.pname = "project.releaseVersion"	# name of property 
        self.currentProperty = self.pname+"="+VersionGetter().versionString+"\n"
        self.needsUpdate = False

    def doSubst(self, dir, fname):
        fn = os.path.abspath(os.path.join(dir, fname))
	if not os.access(fn, os.R_OK):
	    return
	fd = open(fn, 'r')
	lines = []
	for line in fd:
            if(line.startswith(self.pname)):
	        if line != self.currentProperty:
	            self.needsUpdate = True
                    print line, self.currentProperty
	fd.close()

    def main(self):
	dir = os.path.abspath(os.path.expanduser(self.dir))
	fname_re = re.compile(self.fname_re)
	for fname in os.listdir(dir):
            self.needsUpdate = False
	    if fname_re.search(fname) and not fname.endswith("~"):
		self.doSubst(dir, fname)
            if(self.needsUpdate):
                print fname, "\n\nRUN_ETL_UPDATE=Update-ETL"
                sys.exit(0)
        print "ETL data is up to date."

VersionChecker().main()
