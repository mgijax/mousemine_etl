from .AbstractItemDumper import *
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
        a2.accid AS "mgiId",
        a3.accid AS "mgiJnum"
    FROM BIB_Refs r
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
        self.authors = {}
        #
        def makeIdIndex(ldb):
            dups = set()
            q = '''
            select accid
            from acc_accession
            where _logicaldb_key = %d
            and _mgitype_key = 1
            and preferred = 1
            group by accid
            having count(*) > 1
            ''' % ldb
            for r in self.context.sql(q):
                dups.add(r['accid'])

            ix = {}
            q = '''
            select _object_key, accid
            from acc_accession
            where _logicaldb_key = %d
            and _mgitype_key = 1
            and preferred = 1
            ''' % ldb
            for r in self.context.sql(q):
                if r['accid'] not in dups:
                    ix[r['_object_key']] = r['accid']
            return ix
        #
        self.rk2pmid = makeIdIndex(29)
        self.rk2doi  = makeIdIndex(65)

    # Calculates a citation string from the attributes.
    # Default format:
    #           FirstAuthor [etal] (year) Title. Journal vol(issue):pp-p.
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
        rk = r['_refs_key']
        ##########
        # Another temporary hack while we wait for MGI to fix a data problem.
        # Every ref should have exactly 1 preferred J#. Data error: there's a
        # reference with 2, which causes the query to return the same ref twice,
        # which causes a duplicate key error. So for now, swallow the error and
        # skip the record. 
        try:
            r['id'] = self.context.makeItemId('Reference', r['_refs_key'])
        except:
            self.context.log("Failed to create ID for reference. Skipped: " + str(r))
            return None
        # end of hack
        ##########
        r['pubMedId'] = self.rk2pmid.get(rk, None)
        r['doi'] = self.rk2doi.get(rk, None)

        #---------------------------------------
        #
        if r['authors'] is None:
            r['authors'] = ''
        r['firstAuthor'] = ''
        anames = [_f for _f in map(lambda x: x.strip(), r['authors'].split(';')) if _f]
        r['authors'] = anames
        if len(anames) > 0:
            r['firstAuthor'] = anames[0]
        arefs = []
        for a in anames:
            if a not in self.authors:
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

