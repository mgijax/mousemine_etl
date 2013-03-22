from common import *
from DumperContext import DumperContext
import re

class AbstractItemDumper:
    SUPER_RE = re.compile(r'<([^>]+)>')
    def __init__(self, context):
	self.context = context
	self.dumpArgs = None
	self.writeCount = 0
	self.dotEvery = 1000
	self.dotsPerLine = 50

    def superscript(self, s):
        return self.SUPER_RE.sub(r'<sup>\1</sup>',s)

    def quoteLT(self, s):
	"""
	Quotes the "<" characters in a string.
	"""
        return None if s is None else str(s).replace('<', '&lt;')
        
    def quote(self, s):
        """
	Quotes a string, s, so that it is safe to use as a value for an xml attribute.
	"""
	if s is None:
	    return None
	return str(s).replace('&','&amp;').replace('<', '&lt;').replace('"','&quot;')

    def makeRefsFromKeys(self, keys, typename):
	refs = []
	for k in keys:
	    ref = self.context.makeItemRef(typename, k)
	    refs.append('<reference ref_id="%s" />'%ref)
	return refs
	    
    def getWriteCount(self):
        return self.writeCount

    def getClassName(self):
        cn = self.__class__.__name__
	if cn.endswith("Dumper"):
	    cn = cn[:-6]
	return cn

    def getDefaultFileName(self):
        return self.getClassName() + '.xml'

    def constructQuery(self, qtmplt=None, params=None):
	if qtmplt is None:
	    qtmplt = self.QTMPLT
	if params is None:
	    params = self.context.QUERYPARAMS
        return qtmplt % params

    def writeItem(self, r, tmplt=None, i=None):
        for key, value in r.iteritems():
	    if value == "Not Applicable":
	        r[key] = " "
	if tmplt is None:
	    tmplt=self.ITMPLT
	if type(tmplt) is types.StringType:
	     s = tmplt % r
	else:
	    s = tmplt[i] % r
	if self.filter(r, s) is not False:
	    self.context.writeOutput(r['id'],s)
	    self.writeCount += 1

    def _processRecord(self, r, qIndex=None):
	try:
	    self.recordCount += 1
	    if self.dotEvery > 0 and self.recordCount % self.dotEvery == 0:
		nl = (self.recordCount % (self.dotEvery*self.dotsPerLine) == 0)
	        self.context.log('.',timestamp=False,newline=nl)
	    if qIndex is None:
		rr = self.processRecord(r)
	    else:
		rr = self.processRecord(r, qIndex)
	except DumperContext.DanglingReferenceError:
	    return
	else:
	    if rr is not None:
		self.writeItem(rr, None, qIndex)

    def mainDump(self):
	if type(self.QTMPLT) is types.StringType:
	    self.recordCount = 0
	    q = self.constructQuery()
	    if len(q.strip()) > 0:
		self.context.sql(q, self._processRecord)
	    self.context.log('',timestamp=False)
	    self.context.log('Processed %d records.' % self.recordCount)
	else:
	    for i,qt in enumerate(self.QTMPLT):
		self.recordCount = 0
		q = self.constructQuery(qt)
		if len(q.strip()) > 0:
		    self.context.sql(q, self._processRecord, args={'qIndex':i})
		self.context.log('',timestamp=False)
		self.context.log('Processed %d records.' % self.recordCount)

    def dump(self, **kwargs):
	self.context.log('%s: Starting dump. args=%s' %(self.__class__.__name__, str(kwargs)))
	self.dumpArgs = kwargs
	self.fname = kwargs.get('fname',None)
	self.writeCount = 0
	if self.fname:
	    self.context.openOutput(self.fname)
	if self.preDump() == False:
	    return
	self.mainDump()
	self.postDump()
	self.context.log('Total items written: %d' % self.writeCount)
	return self.writeCount

    #==========================================================================

    # Defines the query template for the dumper class. The actual query is created by
    # instantiating the template with self.context.QUERYPARAMS.
    # Each record returned by the query will be passed to processRecord (see below).
    #
    # A subclass may also define QTMPLT as a _list_ of template strings, in which case:
    # (1) each will be instantiated and executed in turn, and (2) processRecord will
    # be passed an additional parameter, which is the 0-based index of the currently
    # executing query.
    #
    # OVERRIDE ME. 
    #
    QTMPLT = ''

    # Defines the item template for the dumper class. Each value returned by
    # self.processRecord() is used as a dict to instantiate this template
    # (the results of which are written out).
    #
    # If QTMPLT is a list, then ITMPLT may also be a list. In this case, records
    # returned by the i-th query use the i-th ITMPLT. (If ITMPLT is a string,
    # all records from all queries use the same template.)
    #
    # OVERRIDE ME.
    #
    ITMPLT = ''

    # Process/modify a record, r, returned by the query.
    # Returns a dict (e.g. r), or None. The dict is used to
    # instantiate the ITMPLT to write to the output.
    # If None is returned, no output is written for the record.
    # When QTMPLT is a list, then the qIndex parameter will be set
    # to the 0-based index of the currently executing query.
    #
    # OVERRIDE ME.
    #
    def processRecord(self, r, qIndex=None):
        return r

    # OVERRIDE ME. Return False to cancel the dump.
    #
    def preDump(self):
    	pass

    # OVERRIDE ME. Perform any dump postprocessing.
    #
    def postDump(self):
    	pass

    # Called just before outputting an item.
    # Return False to cancel outputting that item.
    # OVERRIDE ME.
    #
    def filter(self, r, s):
        return True


