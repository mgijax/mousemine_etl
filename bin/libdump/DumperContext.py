from common import *
import mgidbconnect as db
import time

class DumperContext:

    class ItemError(RuntimeError):
        pass

    class DuplicateIdError(ItemError):
        pass

    class DanglingReferenceError(ItemError):
        pass

    def __init__(self, debug=False, dir=".", limit=None, defs={}, logfile=None, logconsole=True, checkRefs=True):
	self.debug=debug
	self.dir = dir
	self.limit=limit
	self.fname = None
	self.checkRefs = checkRefs
        db.setConnectionFromPropertiesFile()
	self.fd = sys.stdout
	if logfile:
	    self.logfile = os.path.abspath(os.path.join(os.getcwd(), logfile))
	    self.logfd = open(self.logfile, 'a')
	else:
	    self.logfile = "<stderr>"
	    self.logfd = sys.stderr
	self.consolefd = None
	if logconsole and self.logfd is not sys.stderr:
	    self.consolefd = sys.stderr
	self.QUERYPARAMS = {
	    # MGItype keys
	    'REF_TYPEKEY'        : 1,
	    'MARKER_TYPEKEY'     : 2,
            'PROBE_TYPEKEY'      : 3,
            'ANTIBODY_TYPEKEY'   : 6,
            'ASSAY_TYPEKEY'      : 8,
            'IMAGE_TYPEKEY'      : 9,
	    'STRAIN_TYPEKEY'     : 10,
	    'ALLELE_TYPEKEY'     : 11,
	    'GENOTYPE_TYPEKEY'   : 12,
	    'TERM_TYPEKEY'       : 13,
	    'ORGANISM_TYPEKEY'   : 20,
	    'CHROMOSOME_TYPEKEY' : 27,

	    #######################
	    # These are the Organisms we are dumping data for from MGI.
	    # MGI does not represent pahari, caroli, or spretus as organisms per se,
	    # so they are hard-coded here.
	    'ORGANISMS' : {
		# NB: It is important that musculus and human have keys 1 and 2 respectively
		# to match keys in MGI.
		10090: [1, 'Mus musculus',10090],
		9606 : [2, 'Homo sapiens', 9606],
		# For anything else, the keys can be whatever.
		10093: [3, 'Mus pahari',  10093],
		10089: [4, 'Mus caroli',  10089],
		10096: [5, 'Mus spretus', 10096],
	    },
	    # Hard code the mapping from strain name to taxon
	    # Add as many as desired. These are the ones we need for release.
	    'STRAIN_ORGANISM' : {
	        'PAHARI/EiJ' : 10093,
		'CAROLI/EiJ' : 10089,
		'SPRET/EiJ'  : 10096,
	    },
	    #######################

	    # MRK_Types (marker type) keys
	    'GENE_MRKTYPEKEY'	: 1,

	    # Organism keys
	    'MOUSE_ORGANISMKEY' : 1,
	    'HUMAN_ORGANISMKEY' : 2,
	    'RAT_ORGANISMKEY'   : 40,

	    # Logical database keys
	    'MGI_LDBKEY'      : 1,
	    'TAXONOMY_LDBKEY' : 32,
	    'PUBMED_LDBKEY'   : 29,
	    'ENTREZ_LDBKEY'   : 55,
	    'DOI_LDBKEY'      : 65,
	    'SP_LDBKEY'       : 13,
	    'TR_LDBKEY'       : 41,

	    # VOC_Vocab keys
	    'ALLELE_MUTATION_VKEY' : 36,
	    'ALLELE_COLLECTION_VKEY' : 92,
	    'ALLELE_ATTRIBUTE_VKEY' : 93,
	    'STRAIN_ATTRIBUTE_VKEY' : 27,

	    # VOC_Term keys
	    'HYBRID_HOMOL_KEY' : 13764519,

	    # Annotation type keys
	    'ALLELE_ATTRIBUTE_AKEY' : 1014,
	    'STRAIN_ATTRIBUTE_AKEY' : 1009,

	    # MGI_Reference_Assoc, _refassoctype_keys
	    'STRAIN_REFASSOCTYPE_KEYS' : [1009,1010],

	    # Feature relationship category keys 
	    'ALL_FR_CATEGORY_KEYS' : [1002,1003,1004,1001],
	    'ALL_FR_NAME_MAP' : {
		1001 : { # interacts_with
		    'subjectAttrName' : 'interactor',
		    'objectAttrName'  : 'target'
		    },
		1002 : { # cluster_has_member
		    'subjectAttrName' : 'cluster',
		    'objectAttrName'  : 'member'
		    },
		1003 : { # mutation_involves
		    'subjectAttrName' : 'mutation',
		    'objectAttrName'  : 'feature'
		    },
		1004 : { # expresses_component
		    'subjectAttrName' : 'allele',
		    'objectAttrName'  : 'feature'
		    }
	        },

	    # Coordinate maps
	    'HUMAN_MAPKEY' : 47,

	    # Nomen status values (from MRK_Status)
	    'OFFICIAL_STATUS' : 1,
	    'WITHDRAWN_STATUS': 2,

            # MGI_Notetype keys
            'STRAIN_SPECIFIC_NOTETYPE_KEY' : 1035,

	    #
	    'TAXAIDS' : COMMA.join(map(lambda o:"'%d'"%o, TAXAIDS)),
	    'LIMIT_CLAUSE' : limit and (' LIMIT %d '%limit) or '',

	    #
	    'ORGANISMKEYS' : '1,2',

	    #
	    'MGI_HYBRID_HOMOLOGY_KEY' : 13764519,

	    }

	# Keys from ACC_MGIType.
	# Maps type name to type key
	self.TYPE_KEYS = self.loadMgiTypeKeys()
	# Add other types that we need to output
	self.TYPE_KEYS.update({
	    'Homologue'			: 10001,
	    'OrthologueEvidence'	: 10002,
	    'OrthologueEvidenceCode'	: 10003,
	    'Location'			: 10004,
	    'GenotypeAllelePair'	: 10005,
	    'OntologyAnnotation'	: 10006,
	    'OntologyAnnotationEvidence': 10007,
	    'OntologyAnnotationEvidenceCode': 10008,
	    'Synonym'			: 10009,
	    'DataSource'		: 10010,
	    'DataSet'			: 10011,
	    'CrossReference'		: 10012,
	    'Author'			: 10013,
	    'SOTerm'			: 10014,
	    'SyntenicRegion'		: 10015,
	    'AlleleMolecularMutation'	: 10016,
	    'CellLine'			: 10017,
	    'CellLineDerivation'	: 10018,
	    'Expression'                : 10019,
            'EMAPATerm'                 : 10020,
            'AlleleAttribute'           : 10021,
            'DirectedRelationship'      : 10022,
            'DirectedRelationshipProperty' : 10023,
	    'Protein'			: 10024,
            'Comment'                   : 10025,
            'SyntenyBlock'              : 10026,
            'StrainAttribute'           : 10027,
	    })

	# load MGI datadump timestamp from the database
	self.loadMgiDbinfo()

	# map integer type ids to type names
	self.TK2TNAME = dict(map(lambda x: (x[1],x[0]), self.TYPE_KEYS.items()))

	# Maps type name to next-id for allocating IDs.
	# Generally, for a given type, either all IDs are generated
	# or all IDs are constructed from existing MGI keys.
	self.NEXT_ID = { }

	# Two-level map { type -> { mgi-key -> mapped-key }}
	self.KEY_MAP = {}

	#
	# Keep track of item ids that have been written out.
	#
	self.idsWritten = set()

	# apply command-line definitions to the context
	for n,v in defs.iteritems():
	    if not hasattr(self,n):
		setattr(self, n, v)

	#
	# Keep track of files open for output
	# dict : filename -> filedescriptor
	#
	self.outfiles = {}

        # list of non standard publications  
        # aka private or de-emphasized in MGI

        self.unciteablePubs = {}
        self.loadUnciteablePubs()

        self.annotationComments = {}

    # query based on PrivateRefSet.py in femover
    #   the Reference Type Key 31576687 is 'Peer Reviewed Article' (_vocab_key = 131)
    def loadUnciteablePubs(self):
       q = '''select br._Refs_key as _refs_key
              from BIB_Refs br, ACC_Accession acc
              where br._Refs_key = acc._Object_key
              and acc._MGIType_key = 1
              and acc.prefixPart = 'J:'
              and acc._LogicalDB_key = 1
              and br._referencetype_key != 31576687
              '''
       for r in db.sql(q):
         self.unciteablePubs[r['_refs_key']] = 1;

    # returns true if the refKey is not in the list of unciteable reference keys
    #
    def isPubCiteable(self,refKey):
        return not self.unciteablePubs.has_key(refKey)

 
    # Loads datadump timestamp from MGI.
    #
    def loadMgiDbinfo(self):
	self.mgi_dbinfo = None
	q = '''
	SELECT *
	FROM MGI_dbinfo
	'''
	self.mgi_dbinfo = self.sql(q)[0]
	self.mgi_dbinfo['lastdump_date_f'] = self.mgi_dbinfo['lastdump_date'].strftime('%Y-%m-%d')
	self.log('MGI database dump date: %s' % self.mgi_dbinfo['lastdump_date_f'])

    # Loads type information from ACC_MGIType.
    #
    def loadMgiTypeKeys(self):
	tkeys = {}
        q = '''
	    SELECT _mgitype_key, name
	    FROM ACC_MGIType
	    '''
	for r in db.sql(q):
	    tkeys[r['name']] = r['_mgitype_key']
	return tkeys

    # Given a type name and an integer key unique within that 
    # type, creates a globally unique string key of the form
    # "n_m", where:
    #     n is the type, mapped to an integer key
    #     m is local key, mapped to a 0-based sequence within the type.
    # Args:
    #  itemType (string) The ID space in which to generate the key. Determines the "n" part.
    #  localKey (integer) If provided, also creates a mapping from localKey (which is generally
    #    a key value from the MGI database) to the generated key.
    #  exists (boolean) Only applies if localKey provided. If False, there must be no existing
    #    mapping for type+localKey. (Used to generate the id values for new items.) If True, there must
    #    be an existing mapping for type+localKey. (Use for generating values for reference and collection
    #    attributes. If None, no existence check is made.
    # Returns:
    #   An identifier string of the form "n_m"
    #
    def makeGlobalKey(self, itemType, localkey=None, exists=None):
	# 
	n = self.TYPE_KEYS[itemType] if type(itemType) is types.StringType else itemType
	kmap = self.KEY_MAP.setdefault(n, {})
	m = self.NEXT_ID.setdefault(n, 1)
        if localkey is None:
	    # no local key, so no key mapping worries
	    self.NEXT_ID[n] += 1
	elif self.checkRefs and exists is True:
	    # Generating a reference.
	    # Enforce key mapping already exists, and that the object has was actually writtem
	    # and use the mapped key
	    m = kmap.get(localkey, None)
	    if not m or ('%d_%d' % (n,m)) not in self.idsWritten:
	    	raise DumperContext.DanglingReferenceError('itemType=%d, localkey=%d' % (n, localkey))
	elif self.checkRefs and exists is False:
	    # Generating an id. 
	    # Enforce we haven't already seen it (no duplicates)
	    # Increment the counter
	    if localkey in kmap:
	        raise DumperContext.DuplicateIdError('itemType=%d, localkey=%d' % (n, localkey))
	    self.NEXT_ID[n] += 1
	    kmap[localkey] = m
	else:
	    # Don't care, just do the right thing.
	    # If already seen, use the mapped key.
	    # Otherwise, increment the counter.
	    if localkey in kmap:
	        m = kmap[localkey]
	    else:
		kmap[localkey] = m
	        self.NEXT_ID[n] += 1
	id = '%d_%d' % (n,m)
	return id

    def makeItemId(self, itemType, localKey=None):
        return self.makeGlobalKey(itemType, localKey, False)

    def makeItemRef(self, itemType, localKey):
        return self.makeGlobalKey(itemType, localKey, True)

    # Wrapper that logs sql queries.
    #
    def sql(self, q, p=None, args={}):
	self.log(str(q))
        return db.sql(q, p, args=args)

    def sqliter(self, q):
        self.log(str(q))
	return db.sqliter(q)

    def openOutput(self, fname):
	if self.fd and not self.fd.closed:
	    self.fd.flush()
        self.fname = os.path.abspath(os.path.join(self.dir, fname))
        if not os.path.exists(self.dir):
            os.makedirs(self.dir)
	self.fd = self.outfiles.get(self.fname, None)
	if self.fd is None:
	    # open a new output file
	    self.fd = open(self.fname, 'w')
	    self.outfiles[self.fname]=self.fd
	    self.fd.write('<?xml version="1.0"?>\n')
	    self.fd.write('<items>\n')

    def writeOutput(self, id, s):
	self.idsWritten.add( id )
        self.fd.write(s)

    def closeOutputs(self):
	for fname,fd in self.outfiles.items():
	    fd.write('\n</items>\n')
	    fd.close()

    def log(self, s, timestamp=True, newline=True):
	newline = newline and "\n" or ""
	timestamp = timestamp and ("%s :: "%time.asctime()) or ""
	msg = "%s%s%s" % (timestamp, s, newline)
	self.logfd.write(msg)
	self.logfd.flush()
	if self.consolefd:
	    self.consolefd.write(msg)
	    self.consolefd.flush()

    def installSamplers(self, samplermodule):
        for n in dir(samplermodule):
	    x = getattr(samplermodule,n)
	    if type(x) is types.FunctionType:
		print n
	        pass

