#
# refresh.py
#
# Driver script for update/refreshing data files from configured sources,
# Depends on config file: refresh.cfg
#
# Each source (e.g., emapa, go, mgi-base, ...) has its own output directory. 
# Every time a source is refreshed, a new subdirectory is created to hold the results.
# The new directory's name is a timestamp, which is determined by when the refresher was
# started. For example:
#	2014.01.29.14.33.47/
# would be the directory name for a refresh run that started at 2:33:47 in the 
# afternoon on 29 January, 2014.
# If the refresher is updating multiple sources, each source will have a subdirectory 
# with the same name (timestamp). 
#
# Each source has a symlink, "latest", that points to the directory of the latest successful 
# refresh run. This symlink is changed only when a refresh is successful.
#
# Without intervention, the outputs of successive runs simply accumulates indefinitely.
# A data retention policy dictates which runs (directories) are retained and which are deleted.
# The current policy is the simplest viable option, to wit, we keep the "latest" as well as the 
# new directory created by this run. If the run is successful, these are the same; if not, then
# latest still points to the previous run, while the new directory contains the output
# of the failed run.
#

import os
import sys
import time
import re
from ConfigParser import ConfigParser
import logging
from optparse import OptionParser

STRF = "%Y.%m.%d.%H.%M.%S"

class SourceRefresher:
    """
    Refreshes a single source. Includes maintaining the "latest" sym link and cleaning
    up results of old runs.
    """
    def __init__(self, sn, cp):
	self.name = sn
	self.odir = cp.get(sn,'ODIR')
	self.pdir = os.path.dirname(self.odir)
	self.dname = os.path.basename(self.odir)
	self.latest = os.path.join(self.pdir, "latest")
	self.cmd = cp.get(sn,'cmd')
	self.success = None

    def cleanup(self):
	try:
	    latest=os.readlink(self.latest)
	except:
	    latest=None

	logging.info("Latest="+str(latest))
	for fn in os.listdir(self.pdir):
	    if fn == self.dname or fn == latest \
	    or not re.match(r"\d\d\d\d\.\d\d\.\d\d\.\d\d\.\d\d\.\d\d", fn):
	        continue
	    ffn = os.path.join(self.pdir,fn)
	    cmd = "rm -fr %s"%ffn
	    logging.info("Removing directory: " + cmd)
	    os.system(cmd)

    def refresh(self):
	logging.info("%s: starting ..."%self.name)
	logging.info("%s: running command: %s"%(self.name,self.cmd))
	os.makedirs(self.odir)

	# OK, here we go...
	status = os.system(self.cmd)

	if status == 0:
	    # re-link 'latest' to point to new directory
	    c2 = "cd %s; rm -f latest; ln -s %s latest" % (self.pdir,self.dname)
	    logging.info("%s: running command: %s"%(self.name,c2))
	    try:
		os.system(c2)
	    except:
		# failed at the last minute. dang!
		self.success = False
		logging.info("%s: relinking failed!"%self.name)
	    else:
		# yahoo!
		self.success = True
		logging.info("%s: success!"%self.name)
	else:
	    # cmd failed
	    self.success=False
	    logging.info("%s: command failed!"%self.name)
	self.cleanup()

def getOpts():
    op = OptionParser()
    op.add_option(
    	"-s", "--source", dest="sources",
    	default=[], action="append",
	help="Name of a source (repeatable).")
    return op.parse_args()
    
def main():
    opts,args =  getOpts()
    mydir=os.path.dirname(__file__)
    timestamp = time.strftime(STRF,time.localtime(time.time()))
    basedir = os.path.abspath(os.path.join(mydir, '..'))
    configFile =  os.path.join(mydir,'config.cfg') 
    rex = re.compile(r"^\[(.*)\]")

    cp = ConfigParser({"BASEDIR":basedir,"TIMESTAMP":timestamp})
    cp.read(configFile)

    orderedSources = []
    cfd = open(configFile,'r')
    for l in cfd:
        m = rex.match(l)
	if m and m.group(1) != "DEFAULT":
	    orderedSources.append( m.group(1))
    cfd.close()

    if len(opts.sources) > 0:
	orderedSources = filter(lambda x:x in opts.sources, orderedSources)

    lfn = None
    for sn in orderedSources:
        if not cp.has_section(sn):
	    logging.error("Unknown source name: %s"%sn)
	    continue
	if lfn is None:
	    lfn = cp.get(sn,'LOGFILE')
	    logging.basicConfig(
		level=logging.DEBUG,
		format='%(asctime)s %(levelname)s %(message)s',
		filename=lfn,
		filemode='a')
	    logging.info("STARTING SOURCE ETL REFRESH " +40*"-")
	#
	sr = SourceRefresher(sn, cp)
	sr.refresh()
    logging.info("END OF REFRESH " +40*"-")

main()
