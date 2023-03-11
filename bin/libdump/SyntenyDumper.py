#
# SyntenyDumper.py
#
# This script produces a mouse/human synteny mapping from orthology
# and location information in MGI. The results are output in InterMine 
# ItemsXML format.
#
# A "synteny block" is a pair of genomic ranges - one in species A 
# and one in species B - deemed to be "equivalent". 
# A "synteny mapping" is a set of synteny blocks that partitions both
# genomes. That is, in each genome, the blocks (a) are non-overlapping
# and (b) cover the genome.
#
# A synteny block has the following information:
#       id
#       orientation (+=same, -=opposite)
#       chr, start, end for species A
#       chr, start, end for species B
# The two ranges do not include strand, because biologically, the
# blocks represent double-stranded chunks of DNA.
#
# This script builds synteny blocks starting from pairs of orthologs.
#
# Example:
#
# Suppose mouse genes a, b, c are on the + strand of chr 6 from 10 to 11 MB,
# ordered as given. Their human orthologs, A, B, and C, are on the - strand of chr 22
# between 17 and 18.5 MB. We therefore have a synteny block
# comprising mouse 6:10-11MB and human 22:17-18.5MB in the - orientation.
#
# If the strands are the same (both + or both -), the sblock's orientation
# is +. Otherwise (+/- or -/+) the sblock's orientation is -.
#
# Implementation outline.
#       1. Query MGI (adhoc.infomratics.jax.org) for mouse/human ortholog pairs.
#       (Each row has columns for 2 genes, one mouse and one human).
#       For each gene, include its id, symbol, chromosome coordinates, and strand.
#       Sorted on mouse chromosome+start pos.
#       2. Eliminate overlaps. Scan the rows: remove a row if its start pos is
#       less than the end pos of the previous row. Then sort on human coords and
#       repeat for human genes.
#       3. Assign indexes to the human genes (0..n-1). Then re-sort by mouse coordinates
#       and assign indexes to the mouse genes. (Adds two columns to the table.)
#       4. Scan: generating initial synteny blocks. The result of this pass is a set of blocks that
#       have gaps in between them.
#       5. Massage the blocks. Fills in the gaps by extending neighboring blocks toward
#       each other. Also extends blocks at the starts of chromosomes.
#       6. Write the blocks in InterMine ItemXML format. Each block is written as two
#       SyntenicRegion features (one human, one mouse) with two corresponding  Location
#       objects. The output also includes minimal representations for the Organisms and
#       Chromosomes.
#       
#

import sys
import os
import urllib.request, urllib.parse, urllib.error

from .AbstractItemDumper import *
from .DataSourceDumper import DataSetDumper

#
MTAXID = 10090
MCHRS = "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 X Y".split()
#
HTAXID = 9606
HCHRS = "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y".split()
#
SYNTENIC_REGION_SOID = "SO:0005858"
#

class SyntenyDumper(AbstractItemDumper):
    QTMPLT = '''
SELECT distinct
        m1.symbol AS msymbol,
        mch._chromosome_key AS mchr,
        cast(mlc1.startCoordinate AS int) AS mstart,
        cast(mlc1.endCoordinate AS int) AS mend,
        mlc1.strand AS mstrand,
        m2.symbol AS hsymbol,
        hch._chromosome_key AS hchr,
        cast(mlc2.startCoordinate AS int) AS hstart,
        cast(mlc2.endCoordinate AS int) AS hend,
        mlc2.strand AS hstrand
    FROM
        MRK_Cluster mc,
        MRK_ClusterMember mcm,
        MRK_ClusterMember mcm2,
        mrk_marker m1,
        mrk_location_cache mlc1,
        mrk_chromosome mch,
        mrk_marker m2,
        mrk_location_cache mlc2,
        mrk_chromosome hch
    WHERE
    mc._clustersource_key = %(HYBRID_HOMOL_KEY)s
    AND mc._cluster_key = mcm._cluster_key
    AND mcm._cluster_key = mcm2._cluster_key
    AND mcm._marker_key = m1._marker_key
    AND m1._organism_key = 1
    AND m1._marker_key = mlc1._marker_key
    AND mlc1.startCoordinate is not null
    AND mlc1.genomicchromosome = mch.chromosome
    AND mch._organism_key = 1
    AND mcm2._marker_key = m2._marker_key
    AND m2._organism_key = 2
    AND m2._marker_key = mlc2._marker_key
    AND mlc2.startCoordinate is not null
    AND mlc2.genomicchromosome = hch.chromosome
    AND hch._organism_key = 2
    AND mlc1.strand is not null
    AND mlc2.strand is not null
    AND (select count(mx._marker_key) from MRK_ClusterMember mx, mrk_Marker my where mx._marker_key = my._marker_key and my._organism_key = 2 and  mx._cluster_key = mcm._cluster_key) = 1
    AND (select count(mx._marker_key) from MRK_ClusterMember mx, mrk_Marker my where mx._marker_key = my._marker_key and my._organism_key = 1 and  mx._cluster_key = mcm._cluster_key) = 1

    ORDER BY mchr, mstart
    %(LIMIT_CLAUSE)s
    '''

    SYN_REGION_TMPLT = '''
        <item class="SyntenicRegion" id="%(id)s" >
          <attribute name="symbol" value="%(symbol)s" />
          <attribute name="name" value="%(name)s" />
          <reference name="sequenceOntologyTerm" ref_id="%(soref)s"/>
          <reference name="organism" ref_id="%(organism)s" />
          <reference name="chromosome" ref_id="%(chromosome)s" />
          <reference name="chromosomeLocation" ref_id="%(chromosomeLocation)s" />
          <reference name="syntenyBlock" ref_id="%(syntenyBlock)s" />
          <attribute name="orientation" value="%(orientation)s" />
          <collection name="dataSets">%(dataSets)s</collection>
          </item>
    '''

    # Note that SyntenyLocations have coordinates, but NO strand.
    LOC_TMPLT = '''
        <item class="Location" id="%(id)s">
          <reference name="feature" ref_id="%(feature)s" />
          <reference name="locatedOn" ref_id="%(locatedOn)s" />
          <attribute name="start" value="%(start)d" />
          <attribute name="end" value="%(end)d" />
          <attribute name="strand" value="0" />
          </item>
    '''

    # Note that SyntenyBlocks have Syntentic Regions and NO publication.
    SYN_BLOCK_TMPLT = '''
        <item class="SyntenyBlock" id="%(id)s">
          <collection name="syntenicRegions">%(syntenicRegions)s</collection>
          <reference name="dataSet" ref_id="%(dataSet)s" />
          </item>
    '''

    def preDump(self):
        self.mchr2count = {}
        self.CNAME2CID = {}
        self.CID2N = {}
        self.smap = { 'f':'+', 'r':'-', '+':'+', '-':'-'}
        self.allPairs = []
        self.mkey = 1 + self.context.sql('select max(_marker_key) as k from mrk_marker')[0]['k']
        self.soref = self.context.makeGlobalKey('SOTerm',int(SYNTENIC_REGION_SOID.split(":")[1]))
        return True

    def processRecord(self, r):
        # for each mouse/human orthology pair, select the pair and associated data 
        # (e.g., symbol, chromosome, etc.) for each
        if r['mend'] < r['mstart']:
            r['mstart'], r['mend'] = r['mend'], r['mstart']
        r['mstrand'] = self.smap[r['mstrand']]
        if r['hend'] < r['hstart']:
            r['hstart'], r['hend'] = r['hend'], r['hstart']
        r['hstrand'] = self.smap[r['hstrand']]
        self.allPairs.append(r)
        return None
        
    def postDump(self):

        # pairs are already ordered by mouse chr/start.
        # Scan list. Remove any mouse-gene-to-mouse-gene overlaps.
        lastPair = None
        tmp = []
        for pair in self.allPairs:
            if lastPair and lastPair['mchr'] == pair['mchr'] and lastPair['mend'] > pair['mstart']:
                continue
            tmp.append(pair)
            lastPair = pair
        self.allPairs = tmp

        # Sort by human chr/start coords and remove any overlapping genes.
        self.allPairs.sort(key=lambda x:(x['hchr'], x['hstart']))
        lastPair = None
        tmp = []
        for pair in self.allPairs:
            if lastPair and lastPair['hchr'] == pair['hchr'] and lastPair['hend'] > pair['hstart']:
                continue
            tmp.append(pair)
            lastPair = pair
        self.allPairs = tmp

        # Assign human index order
        for (i, pair) in enumerate(self.allPairs):
            pair['iHi'] = i

        # re-sort on mouse chr/start coords and assign index
        self.allPairs.sort(key=lambda x:(x['mchr'], x['mstart']))
        for (i, pair) in enumerate(self.allPairs):
            pair['iMi'] = i

        # scan the list and generate synteny blocks
        blks = self.generateBlocks(self.allPairs)

        # extends chromosome ends
        #blks = self.massageBlocks(blks)

        # output
        for b in blks:
            self.writeBlock(b)

    def startBlock(self,pair):
        """
        Starts a new synteny block from the given ortholog pair.
        Args:
            pair        (dict)  A query result row containing a mouse/human
                    ortholog pair.
        Returns:
            A new synteny block:
                block name  = look like "chr_num", e.g., "17_56"
                orientation = +1 or -1. The first ortholog pair in the block sets this.
                block count = # ortholog pairs making up the block
                mouse/human pair = copy of the result row (list of strings). This initializes our block.
        """
        ori = (pair['mstrand'] == pair['hstrand']) and +1 or -1
        mchr = pair['mchr']
        n = self.mchr2count.setdefault(mchr,1)
        self.mchr2count[mchr] += 1
        bname = 'SynBlock:mmhs:%s_%d' %(mchr, n)
        blockCount = 1
        return [ bname, ori, blockCount, pair.copy() ]

    def extendBlock(self,currPair,currBlock):
        """
        Extends the given synteny block to include the coordinate
        ranges of the given ortholog pair.
        """
        bname,ori,blockCount,pair = currBlock
        currBlock[2] = blockCount+1
        pair['mstart'] = min(pair['mstart'], currPair['mstart'])
        pair['mend']   = max(pair['mend'],   currPair['mend'])
        pair['hstart'] = min(pair['hstart'], currPair['hstart'])
        pair['hend']   = max(pair['hend'],   currPair['hend'])
        pair['iHi'] = currPair['iHi']

    def canMerge(self,currPair,currBlock):
        """
        Returns True iff the given ortholog pair can merge with (and extend)
        the given synteny block. 
        """
        if currBlock is None:
            return False
        bname,ori,bcount,cbfields = currBlock
        cori = (currPair['mstrand']==currPair['hstrand']) and 1 or -1
        return currPair['mchr'] == cbfields['mchr'] \
            and currPair['hchr'] == cbfields['hchr'] \
            and ori == cori \
            and currPair['iHi'] == cbfields['iHi']+ori

    def generateBlocks(self, allPairs, tagPairs=True):
        """
        Scans the pairs, generating synteny blocks.
        """
        blocks = []
        currBlock = None
        for currPair in allPairs:
            if self.canMerge(currPair,currBlock):
                self.extendBlock(currPair,currBlock)
            else:
                currBlock = self.startBlock(currPair)
                blocks.append(currBlock)
            if tagPairs:
                currPair['block'] = currBlock[0]
        return blocks

    def _massageBlocks(self,blocks, x):
        for i in range(len(blocks)-1):
            # cb=current block, nb=next block
            cbname, cbori, cbcount, cbfields = blocks[i]
            nbname, nbori, nbcount, nbfields = blocks[i+1]
            if i == 0:
                cbfields[x+'start'] = 1
            if cbfields[x+'chr'] != nbfields[x+'chr']:
                nbfields[x+'start'] = 1
            else:
                delta = nbfields[x+'start'] - cbfields[x+'end']
                epsilon = 1-delta%2
                cbfields[x+'end']   += delta/2
                nbfields[x+'start'] -= (delta/2 - epsilon)
        return blocks

    # Extends first block on a chromosome to begin at 1.
    # Extends neighboring blocks toward each other so they abut.
    # TODO: Extends last block on a chromosome to the end.
    def massageBlocks(self, blocks):
        blocks = self._massageBlocks(blocks, 'm')
        blocks.sort(key=lambda b:(b[-1]['hchr'], b[-1]['hstart']))
        blocks = self._massageBlocks(blocks, 'h')
        blocks.sort(key=lambda b:(b[-1]['mchr'], b[-1]['mstart']))
        return blocks

    def writeBlock(self, block):
        bname, ori, bcount, fields = block
        ihi = fields['iHi']
        if ori==1:
            ihi -= (bcount-1)
        b = [
          fields['mchr'],
          fields['mstart'],
          fields['mend'],
          fields['hchr'],
          fields['hstart'],
          fields['hend'],
          bname,
          (ori==1 and "+" or "-"),
          bcount,
          fields['iMi'],
          ihi,
        ]
        self.writeBlockItems(*(b[0:8]))

    def writeBlockItems(self, mchr,mstart,mend,hchr,hstart,hend,name,ori):
        (mr,ml) = self.makeSyntenicRegion(1,mchr,mstart,mend,name,ori)
        (hr,hl) = self.makeSyntenicRegion(2,hchr,hstart,hend,name,ori)
        sb = self.makeSyntenyBlock(hr['id'], mr['id'])
        mr['syntenyBlock'] = sb['id']
        hr['syntenyBlock'] = sb['id']
        self.writeItem(mr, self.SYN_REGION_TMPLT)
        self.writeItem(ml, self.LOC_TMPLT)
        self.writeItem(hr, self.SYN_REGION_TMPLT)
        self.writeItem(hl, self.LOC_TMPLT)
        self.writeItem(sb, self.SYN_BLOCK_TMPLT)


    def getDataSetId(self):
        return DataSetDumper(self.context).dataSet(name="Inferred Synteny Blocks from MGI")

    def getDataSetRef(self):
        dsid = self.getDataSetId()
        return '<reference ref_id="%s" />'%dsid

    def makeSyntenicRegion(self, org,chr,start,end,bname,ori):
        # creates and returns a tuple (r,l) where r = a dict suitable for use with SYN_REGION_TMPLT and 
        # l = a dict suitable for use with LOC_TMPLT.
        oid = self.context.makeItemRef('Organism', org)
        cid = self.context.makeItemRef('Chromosome', chr)
        fid = self.context.makeItemId('SyntenicRegion', self.mkey)
        lid = self.context.makeItemId('Location', self.mkey)
        self.mkey += 1
        r = {
            'id' : fid,
            'organism' : oid,
            'symbol' : bname,
            'name' : 'Mouse/Human Synteny Block %s' % bname,
            'chromosome' : cid,
            'chromosomeLocation' : lid,
            'soref' : self.soref,
            'orientation' : ori,
            'dataSets' : self.getDataSetRef(),
            }
        l = {
            'id' : lid,
            'feature' : fid,
            'locatedOn' : cid,
            'start' : start,
            'end' : end,
            }
        return (r,l)

    def makeSyntenyBlock(self, hrid, mrid):
        # creates and returns a dict where b is suitable for use with SYN_BLOCK_TMPLT
        bid = self.context.makeItemId('SyntenyBlock', self.mkey)
        self.mkey += 1
        sr_refs = '<reference ref_id="%s" />'%hrid + '<reference ref_id="%s" />'%mrid
        dsid = self.getDataSetId()
        b = {
            'id' : bid,
            'syntenicRegions' : sr_refs,
            'dataSet' : dsid,
            }
        return b
