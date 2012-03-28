from AbstractItemDumper import *
import string

class PublicationDumper(AbstractItemDumper):
    QTMPLT = ['''
    SELECT 
        r._refs_key, 
	r.abstract,
	a.accid AS pubmedid,
	a2.accid AS mgiid
    FROM BIB_Refs r INNER JOIN ACC_Accession a 
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
    ''','''
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
	r.abstract,
	a2.accid AS mgiid
    FROM BIB_Refs r
      INNER JOIN ACC_Accession a2
      ON r._refs_key = a2._Object_key
      AND a2._logicaldb_key = %(MGI_LDBKEY)d
      AND a2._mgitype_key = %(REF_TYPEKEY)d
      AND a2.preferred = 1
      AND a2.private = 0
      AND a2.prefixPart='MGI:'
    WHERE r._refs_key not in
        (SELECT _object_key
        FROM ACC_Accession
	WHERE _logicaldb_key = %(PUBMED_LDBKEY)d
	AND _mgitype_key = %(REF_TYPEKEY)d
	AND preferred = 1
	AND private = 0)
    %(LIMIT_CLAUSE)s
    ''']
    ITMPLT = ['''
    <item class="Publication" id="%(id)s">
      <attribute name="pubMedId" value="%(pubmedid)s" />
      <attribute name="mgiId" value="%(mgiid)s" />
      %(abstract)s </item>
    ''','''
    <item class="Publication" id="%(id)s">
      <attribute name="mgiId" value="%(mgiid)s" />
      %(attrs)s
      <collection name="authors">%(authors)s</collection>
      </item>
    ''','''
    <item class="Author" id="%(id)s">
        <attribute name="name" value="%(name)s" />
	</item>
    ''']
    def preDump(self):
        self.authors = {}

    def processRecord(self,r, iq):
	if iq == 0:
	    # pubs with pubmed id will be filled out later (by update-publications).
	    # here, we just output the skeleton, containing the pubmed id.
	    # also include abstracts (not filled in by update-publications)
	    r['id'] = self.context.makeItemId('Reference', r['_refs_key'])
	    if r['abstract'] is None:
	        r['abstract'] = ''
	    else:
		r['abstract'] = '<attribute name="abstractText" value="%s"/>' % self.quote(r['abstract'])
	    return r
	elif iq == 1:
	    # Pubs without pubmed ids are filled out from data in MGI.
	    r['id'] = self.context.makeItemId('Reference', r['_refs_key'])
	    r['abstractText'] = r['abstract']
	    if r['title2']:
		r['title'] += r['title2']
	    if r['authors'] is None:
		r['authors'] = ''
	    if r['authors2']:
		r['authors'] += r['authors2']
	    anames = map(string.strip, r['authors'].split(';'))
	    r['firstAuthor'] = anames[0]
	    arefs = []
	    for a in anames:
	        if not self.authors.has_key(a):
		    self.authors[a] = self.context.makeGlobalKey('Author')
		arefs.append('<reference ref_id="%s"/>'%self.authors[a])
	    r['authors'] = ''.join(arefs)
	    attrs = []
	    for n in ['title','journal','volume','issue','pages','year','firstAuthor', 'abstractText']:
	        if r[n]:
		    attrs.append('<attribute name="%s" value="%s"/>'%(n, self.quote(r[n])))
	    r['attrs'] = ''.join(attrs)
	    return r

    def postDump(self):
	lst = self.authors.items()
	lst.sort(key=lambda x:x[1])
	for aname,aid in lst:
	    r = {'id':aid, 'name':self.quote(aname) }
	    self.writeItem( r, self.ITMPLT[2] )

