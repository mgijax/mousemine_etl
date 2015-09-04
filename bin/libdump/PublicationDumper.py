from AbstractItemDumper import *
import string

class PublicationDumper(AbstractItemDumper):
    QTMPLT = '''
    SELECT 
        r._refs_key, 
	r.authors, 
	r.title, 
	r.journal, 
	r.vol AS volume, 
	r.issue, 
	r.date, 
	r.year, 
	r.pgs AS pages,
	r.abstract AS "abstractText",
	a.accid AS "pubMedId",
	a2.accid AS "mgiId",
	a3.accid AS "mgiJnum",
        a4.accid as "doi"
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
      INNER JOIN ACC_Accession a3
      ON r._refs_key = a3._Object_key
      AND a3._logicaldb_key = %(MGI_LDBKEY)d
      AND a3._mgitype_key = %(REF_TYPEKEY)d
      AND a3.preferred = 1
      AND a3.private = 0
      AND a3.prefixPart='J:'
      LEFT OUTER JOIN ACC_Accession a4
      ON r._refs_key = a4._Object_key
      AND a4._logicaldb_key = %(DOI_LDBKEY)d
      AND a4._mgitype_key = %(REF_TYPEKEY)d
      AND a4.preferred = 1
      AND a4.private = 0
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Publication" id="%(id)s">
      <attribute name="mgiId" value="%(mgiId)s" />
      <attribute name="mgiJnum" value="%(mgiJnum)s" />
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
	self.doiDups = {}
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

	q = '''
	    select a._object_key, count(a.accID) as n  from ACC_Accession a
	    where a._LogicalDB_key = 65
	    and a._mgitype_key =1
            and a.preferred = 1
            and a.private = 0
            group by a._object_key 
            having count(a.accID) > 1
            '''
	for r in self.context.sql(q):
	    if r['n'] == 2:
		self.doiDups[ r['_object_key'] ] = 1

    # Calculates a citation string from the attributes.
    # Default format:
    #		FirstAuthor [etal] (year) Title. Journal vol(issue):pp-p.
    #
    def calcCitation(self, r):
	pieces = []
	if r['firstAuthor']:
	    pieces.append(r['firstAuthor'])
	if len(r['authors']) > 1:
	    pieces[0] += ", et al."
	pieces.append(" ")
	if r['year']:
	    pieces.append( '(%d)'%r['year'] )
	pieces.append(" ")
	if r['title']:
	    pieces.append( r['title'] )
	    if not r['title'].endswith('.'):
		pieces.append(".")
	    pieces.append(" ")
	if r['journal']:
	    pieces.append( r['journal'] )
	    pieces.append(" ")
	if r['volume'] :
	    pieces.append( r['volume'] )
	if r['issue']:
	    pieces.append( '(%s)'%r['issue'] )
	if r['pages']:
	    pieces.append( ":" + r['pages'])
	tmplt = '%(firstAuthor)s%(etal)s (%(year)d) %(title)s. %(journal)s %(volume)s(%(issue)s):%(pages)s.'
	return ''.join(pieces)

    def processRecord(self,r):
	attrs = []
	#--------------------------------------
        # For duplicate DOIs
	rk = r['_refs_key']
        ids = self.doiDups.keys()
        if rk in ids:
	    if self.doiDups[rk] == False:
		self.context.log("Skipping %s due to duplicate DOI" % (rk))
		return None
	    else:
		self.context.log("Including %s but duplicate DOIs exist" % (rk));
		self.doiDups[rk] = False;

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
	#
	if r['authors'] is None:
	    r['authors'] = ''
	r['firstAuthor'] = ''
	anames = filter(None, map(string.strip, r['authors'].split(';')))
	r['authors'] = anames
	if len(anames) > 0:
	    r['firstAuthor'] = anames[0]
	arefs = []
	for a in anames:
	    if not self.authors.has_key(a):
            	if r['pubMedId'] is None:
			self.authors[a] = self.context.makeGlobalKey('Author')
			arec = {'id':self.authors[a], 'name':self.quote(a)}
			self.writeItem( arec, self.ATMPLT )
	    		arefs.append('<reference ref_id="%s"/>'%self.authors[a])
	r['citation'] = self.calcCitation(r)
        if r['pubMedId'] is None:
	    attrs.append('<collection name="authors">%s</collection>' % ''.join(arefs))
	flds = (r['pubMedId'] and ['pubMedId'] or []) + \
	  ['citation','title','journal','volume','issue','pages','year','firstAuthor', 'abstractText', 'doi']
	# if there is a pubMedId all other fields will be looked up from PubMed
        if r['pubMedId'] is not None:
		flds = (r['pubMedId'] and ['pubMedId'] or []) + ['citation','abstractText','doi']
	for n in flds:
	    if r[n]:
		attrs.append('<attribute name="%s" value="%s"/>'%(n, self.quote(r[n])))
	r['attrs'] = '\n'.join(attrs)
	return r

