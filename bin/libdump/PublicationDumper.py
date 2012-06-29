from AbstractItemDumper import *
import string

class PublicationDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
        r._refs_key, 
	r.authors, 
	r.authors2, 
	r.title, 
	r.title2, 
	r.journal, 
	r.vol AS volume, 
	r.issue, 
	r.date, 
	r.year, 
	r.pgs AS pages,
	r.abstract AS "abstractText",
	a.accid AS "pubMedId",
	a2.accid AS "mgiId"
    FROM BIB_Refs r
      LEFT OUTER JOIN ACC_Accession a
      ON r._refs_key = a._Object_key
      AND a._logicaldb_key = %(PUBMED_LDBKEY)d
      AND a._mgitype_key = %(REF_TYPEKEY)d
      AND a.preferred = 1
      AND a.private = 0
      INNER JOIN ACC_Accession a2
      ON r._refs_key = a2._Object_key
      AND a2._logicaldb_key = %(MGI_LDBKEY)d
      AND a2._mgitype_key = %(REF_TYPEKEY)d
      AND a2.preferred = 1
      AND a2.private = 0
      AND a2.prefixPart='MGI:'
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Publication" id="%(id)s">
      <attribute name="mgiId" value="%(mgiId)s" />
      %(attrs)s
      </item>
    '''
    ATMPLT = '''
    <item class="Author" id="%(id)s">
        <attribute name="name" value="%(name)s" />
	</item>
    '''
    def preDump(self):
	self.trueDups = {}
	self.badPmids = []
        self.authors = {}
	#
	# Error case: we have discovered duplicate publications in MGD, i.e., different 
	# records with different MGI# (and different J#S), but the SAME pubmed id, same title,
	# authors, journal, etc., etc. This will cause the downstream update_publications
	# step to choke.  Check for/collapse duplicate pubmed ids.
	#
	# Query for PubMed ids that are attached to multiple reference records.
	#
	# There are 2 classes of duplicates:
	#	1. True duplicates, entered in error.
	#	2. The pubmed id represents a conference proceedings or a book, and mgi contains
	#	   multiple papers/chapters. These are represented as individual MGI references, all 
	#          sharing the same pubmed id.
	# For the first class, need to dynamically merge the references. This means (a) outputting only 
	# one Publication object, and (b) mapping references (ie keys) to the one Publication.
	# For the second class, we will simply ignore (not output) the pubmed id.
	# Empirically, the two classes correspond to the counts: for class 1, the pubmed id appears 
	# exactly twice, while for class 2, it appears more than twice.
	#
	q = '''
	    select a.accid, count(a._object_key) as n
	    from acc_accession a
	    where  a._logicaldb_key=29
	    group by a.accid
	    having count(a._object_key) > 1
	    '''
	for r in self.context.sql(q):
	    if r['n'] == 2:
		self.trueDups[ r['accid'] ] = None
		self.context.log("Duplicate PubMed id detected (error case):" + r['accid'])
	    else:
		self.context.log("Duplicate PubMed id detected (legit, but ignored):" + r['accid'])
		self.badPmids.append( r['accid'] )

    def processRecord(self,r):
	attrs = []
	r['id'] = self.context.makeItemId('Reference', r['_refs_key'])

	#---------------------------------------
	# Special processing for dups
	p = r['pubMedId']
	if p in self.badPmids:
	    r['pubMedId'] = None
	elif p in self.trueDups:
	    if self.trueDups[p] is None:
		# first one wins
	        self.trueDups[p] = r['id']
	    else:
		# later ones map to first one
		self.context.ID_MAP[r['id']] = self.trueDups[p]
		self.context.log("Registering mapping: %s --> %s" % (r['id'],self.trueDups[p]))
	        return None
	#---------------------------------------
	    
	if r['pubMedId']:
	    # pubs with pubmed id will be filled out later (by update-publications).
	    # here, we just output the skeleton, containing the pubmed id.
	    # also include abstracts (not filled in by update-publications)
	    #
	    flds = ['pubMedId','abstractText']
	else:
	    # Pubs without pubmed ids are filled out from MGI data.
	    #
	    if r['title2']:
		r['title'] += r['title2']
	    if r['authors'] is None:
		r['authors'] = ''
	    if r['authors2']:
		r['authors'] += r['authors2']
	    r['firstAuthor'] = ''
	    anames = filter(None, map(string.strip, r['authors'].split(';')))
	    if len(anames) > 0:
		r['firstAuthor'] = anames[0]
	    arefs = []
	    for a in anames:
		if not self.authors.has_key(a):
		    self.authors[a] = self.context.makeGlobalKey('Author')
		    arec = {'id':self.authors[a], 'name':self.quote(a)}
		    self.writeItem( arec, self.ATMPLT )
		arefs.append('<reference ref_id="%s"/>'%self.authors[a])
	    r['authors'] = ''.join(arefs)
	    attrs.append('<collection name="authors">%s</collection>' % r['authors'])
	    flds = ['title','journal','volume','issue','pages','year','firstAuthor', 'abstractText']
	#
	for n in flds:
	    if r[n]:
		attrs.append('<attribute name="%s" value="%s"/>'%(n, self.quote(r[n])))
	r['attrs'] = '\n'.join(attrs)
	return r

    def xpostDump(self):
	lst = self.authors.items()
	lst.sort(key=lambda x:x[1])
	for aname,aid in lst:
	    r = {'id':aid, 'name':self.quote(aname) }
	    self.writeItem( r, self.ATMPLT )

