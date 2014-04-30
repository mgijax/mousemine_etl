
'''
DerivedAnnotationHelper.py


The Situation: in MGI, genes are connected to alleles, alleles are connected to genotypes,
and genotypes are annotated to MP and OMIM terms. HOWEVER, just because there is a path from 
a given gene to a given MP or OMIM term does NOT necessarily imply that we should associate the
term with the gene. (As in, what genes are associated with this disease? or what phenotypes are
associated with these genes?)

The Question: when can we infer such a relationship? A set of business rules have been defined
and implemented in the MGI front end.  In MouseMine, we will actually store these associations
as new OntologyAnnotation objects. This helper class serves as the heart of this process.
It implements the same rules as in the MGI front end, and
it builds an index that maps each gene (marker key) to a set of diseases and/or phenotypes
(term keys) to which it should be associated. For each marker/term pair, the index also 
lists the base annotations (VOC_Annot keys) from which the new association is derived.

        -----------------

The HDMC implements a set of rules that try to "reduce" a genotype to a single gene.
If such a reduction can be done, then all the phenotypes and diseases annotated to the
genotype are considered associated with the gene.

How to "reduce" a genotype to a single gene.
Imagine a genotype. It has a collection of alleles associated with
it. For this, we don't care about allele pairs - just throw 'em all into a
bag. Now remove from the bag:
    - all wildtype alleles
    - all alleles of Gt(ROSA)26Sor
    - all transgenic reporter alleles
    - all recombinase alleles (but only if the genotype is 
      marked as conditional)

If the remaining allele(s) are associated with exactly one gene, we would
consider that gene to be "causative" (big quotes but I can't think of a
better word). We would associate that gene with any/all diseases and
phenotypes that are annotated to the genotype (excluding NOTs and
Normals). That means that searching by a disease or phenotype would return
that gene, and conversely, searching by the gene would return those
diseases/phenotypes.

For alleles, we would simply take all remaining alleles in the bag,
regardless of how many genes there are. Each would be associated with
any/all diseases and phenotypes (in the sense of searching).

Small twist: There is another set of disease associations in MGI that attach
directly to an allele, no genotype involved. (annotation type key 1012). Any disease 
associated to an allele in this way should also be associated with the allele's gene.

'''

import mgiadhoc as db
class DerivedAnnotationHelper:

    def __init__(self, context):
	self.context = context
	self.g2m = {}
	self.g2a = {}
	#   { type -> { key -> { vocabkey -> { termkey -> [{refskeys},{annotkeys}] }}}}
	self.k2annots = {}
	self._loadGenotypeIndexes()
	self._loadGenotypeAnnotations()
	self._loadAlleleAnnotations()

    def __addkeys__(self, type, k, vk, tk, rk,ak):
        vti = self.k2annots.setdefault(type,{}).setdefault(k,{}).setdefault(vk,{})
	ti = vti.get(tk,None)
	if not ti:
	    ti = {'refs':set(),'annots':set()}
	    vti[tk] = ti
	ti['refs'].add(rk)
	ti['annots'].add(ak)

    # The main entry point. Iterates over results. Specify "Allele" or "Marker".
    # Yields a sequence of tuples (of database keys)
    # from which to generate inferred/derived gene-MP/OMIM annotations
    def iterAnnots(self, which):
	for k, kdata  in self.k2annots[which].iteritems():
	    for vk, tdata in kdata.iteritems():
		for tk, arks in tdata.iteritems():
		    yield (k, vk, tk, arks)

    ###############################################
    def _loadGenotypeIndexes(self):
	# Step 1. Get all geno+allele+marker key combos, where the allele qualifies.
	# Create 2 indexes: one from genotype to single marker, and another from genotype 
	# to alleles.
	# 

	q1 = '''
	SELECT
	    g._genotype_key, a._allele_key, a._marker_key, m.symbol
	FROM
	    GXD_Genotype g,
	    GXD_AlleleGenotype ag,
	    ALL_Allele a,
	    MRK_Marker m
	WHERE
		g._genotype_key = ag._genotype_key
	    AND ag._allele_key = a._allele_key
	    AND a._marker_key = m._marker_key
	    AND a.isWildType = 0                /* not wild type */
	    AND a._marker_key != 37270          /* not Gt(ROSA)26Sor */
	    AND a._allele_type_key != 847129    /* not transgenic reporter */
	    AND NOT (g.isConditional = 1        /* not recombinase (cond'l genotypes only) */
		AND a._allele_key IN (
		     SELECT _object_key         /* recombinase == has a driver note */
		     FROM MGI_Note
		     WHERE _notetype_key = 1034))
	    '''

	def p1(r):
	    gk = r['_genotype_key']
	    # genotype-to-marker
	    if not self.g2m.has_key(gk):
		# first marker seen for this geno
		self.g2m[gk] = r['_marker_key']
	    else:
		# oops, multiples not allowed. NO marker for you!!
		self.g2m[gk] = None
	    # genotype-to-alleles
	    self.g2a.setdefault(gk, []).append(r['_allele_key'])

	self.context.sql(q1,p1)

    ###############################################
    def _loadGenotypeAnnotations(self):
	# Step 2. Select existing (base) pheno and disease annotations. Use geno key to
	# map to a gene (or not). Create an index mapping gene keys to dictionaries, 
	# which map term keys to lists of refs keys:
	#	{ markerkey -> { vocabkey -> { termkey -> { refkey } } } }
	# Do the same for alleles. The second index has the form:
	#	{ allelekey -> { vocabkey -> { termkey -> { refkey } } } }
	#
	q2 = '''
	    SELECT
		va._object_key, vt._term_key, vt._vocab_key, ve._refs_key, va._annot_key
	    FROM
		VOC_Annot va,
		VOC_Evidence ve,
		VOC_Term vt
	    WHERE
		va._annottype_key in (1002,1005)	/* MP-Geno, OMIM-Geno */
		AND va._annot_key = ve._annot_key
		AND va._term_key = vt._term_key
		AND va._qualifier_key not in (2181424, 1614157) /* not "Normal" or "NOT" */
	    '''

	def p2(r):
	    gk = r['_object_key']
	    tk = r['_term_key']
	    vk = r['_vocab_key']
	    rk = r['_refs_key']
	    ak = r['_annot_key']
	    mk = self.g2m.get(gk, None)
	    if mk:
		self.__addkeys__('Marker',mk,vk,tk,rk,ak)
		#self.m2t2annots.setdefault(mk,{}).setdefault(vk,{}).setdefault(tk,set()).add(r['_refs_key'])
	    for ak in self.g2a.get(gk,[]):
		self.__addkeys__('Allele',ak,vk,tk,rk,ak)
		#self.a2t2annots.setdefault(ak,{}).setdefault(vk,{}).setdefault(tk,set()).add(r['_refs_key'])
	self.context.sql(q2,p2)

    ###############################################
    def _loadAlleleAnnotations(self):
	# Step 3. Fold in the direct allele-disease annotations. Only do this for markers. (No need to 
	# propagate these annotations to Alleles - they're already there.)
	q3 = '''
	    SELECT
		ve._refs_key, a._allele_key, a._marker_key, vt._term_key, vt._vocab_key, va._annot_key
	    FROM
		VOC_Annot va,
		VOC_Evidence ve,
		VOC_Term vt,
		ALL_Allele a
	    WHERE
		va._annottype_key = 1012
		AND va._annot_key = ve._annot_key
		AND va._term_key = vt._term_key
		AND va._object_key = a._allele_key
	    '''

	def p3(r):
	    ak = r['_allele_key']
	    mk = r['_marker_key']
	    tk = r['_term_key']
	    vk = r['_vocab_key']
	    rk = r['_refs_key']
	    ak = r['_annot_key']
	    if mk:
		self.__addkeys__('Marker',mk,vk,tk,rk,ak)
		#self.m2t2annots.setdefault(mk,{}).setdefault(vk,{}).setdefault(tk,set()).add(r['_refs_key'])

	self.context.sql(q3,p3)


####

def __test__():
    import mgiadhoc as db
    class Cxt:
	def sql(self, q, p):
	    return db.sql(q,p)
    return DerivedAnnotationHelper(Cxt())

if __name__== "__main__":
    dah = __test__()
