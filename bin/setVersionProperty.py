#
# setVersionProperty.py
#
# Sets the project.releaseVersion attribute of property files found in ~/.intermine.
#

import sys
import re
import os
from libdump import mgidbconnect as db
from optparse import OptionParser

class VersionGetter:
    def __init__(self):
        self.fname = 'lastdump_date'
        self.query = 'select %s from mgi_dbinfo' % self.fname
        self.dtmplt= '%Y-%m-%d'
        db.setConnectionFromPropertiesFile()
        self.date = db.sql(self.query)[0][self.fname]
        self.versionString = 'MGI update: %s' % self.date.strftime(self.dtmplt)

class VersionSetter:
    def __init__(self):
        self.fname_re = r"\.properties"         # regex to match file names
        self.dir = "~/.intermine"               # directory to look in
        self.pname = "project.releaseVersion"   # name of property to replace
        self.version = VersionGetter().versionString

    def doSubst(self, dir, fname, varname, value):
        fn = os.path.abspath(os.path.join(dir, fname))
        if not os.access(fn, os.R_OK|os.W_OK):
            return
        fd = open(fn, 'r')
        line2 = self.pname+"="+value+"\n"
        #
        lines = []
        for line in fd:
            if line.startswith(self.pname):
                lines.append(line2)
            else:
                lines.append(line)
        fd.close()
        #
        s = "".join(lines)
        fd = open(fn, 'w')
        fd.write(s)
        fd.close()

    def main(self):
        dir = os.path.abspath(os.path.expanduser(self.dir))
        fname_re = re.compile(self.fname_re)
        for fname in os.listdir(dir):
            if fname_re.search(fname) and not fname.endswith("~"):
                self.doSubst(dir, fname, self.pname, self.version)


VersionSetter().main()
