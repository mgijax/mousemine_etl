from AbstractItemDumper import *
from collections import defaultdict 
from OboParser import OboParser

class ExpressionDumper(AbstractItemDumper):

    # Pre-loads as much information about the assay. If the (gel) assay has an image, we store it now.
    # The assay structure is a dict of dict.
    def loadAssay(self):
        q = self.constructQuery('''
            SELECT a._assay_key, a._marker_key, a._refs_key, acc.accid, acc2.accid AS image, at.assaytype
            FROM gxd_assay a
              JOIN acc_accession acc 
                ON a._assay_key = acc._object_key 
              JOIN gxd_assaytype at 
                ON a._assaytype_key = at._assaytype_key
              LEFT OUTER JOIN img_imagepane ip
                ON a._imagepane_key = ip._imagepane_key
              LEFT OUTER JOIN acc_accession acc2
                ON (ip._image_key = acc2._object_key
                AND acc2._logicaldb_key = 1
                AND acc2._mgitype_key = %(IMAGE_TYPEKEY)d
                AND acc2.preferred = 1
                AND acc2.private = 0 )
            WHERE acc._logicaldb_key = 1
            AND acc._mgitype_key = %(ASSAY_TYPEKEY)d
            AND acc.preferred = 1
            AND acc.private = 0
            AND NOT a._assaytype_key IN (10, 11)
            ''')

        for r in self.context.sql(q):
            ak = r['_assay_key']
            self.assay[ak]['gene'] = self.context.makeItemRef('Marker', r['_marker_key'])
            self.assay[ak]['publication'] = self.context.makeItemRef('Reference', r['_refs_key'])
            self.assay[ak]['assayid'] = r['accid']
            self.assay[ak]['assaytype'] = r['assaytype']

            if r['image'] is not None:
                self.assay[ak]['image'] = r['image'] 
        return


    # Assay's can only have one probe / antibody
    def loadProbe(self, assay_key, accid):
        if 'probe' in self.assay[assay_key]:
            print("Error - Expression Dumper: AssayKey: ", assay_key, " has two probes: ", 
                  self.assay[assay_key]['probe'], " and " , accid)
        else:
            self.assay[assay_key]['probe'] = accid
        return


    def loadProbePrep(self):
        q = self.constructQuery('''
            SELECT a._assay_key, acc.accid
            FROM gxd_assay a, gxd_probeprep pp, acc_accession acc
            WHERE a._probeprep_key = pp._probeprep_key
            AND pp._probe_key = acc._object_key
            AND acc._mgitype_key = %(PROBE_TYPEKEY)d
            AND acc._logicaldb_key = 1
            AND acc.preferred = 1
            AND acc.private = 0
            AND NOT a._assaytype_key IN (10, 11)
            ''')

        for r in self.context.sql(q):
            self.loadProbe(r['_assay_key'], r['accid'])
        return


    def loadAntibodyPrep(self):
        q = self.constructQuery('''
            SELECT a._assay_key, acc.accid
            FROM gxd_assay a, gxd_antibodyprep ap, acc_accession acc
            WHERE a._antibodyprep_key = ap._antibodyprep_key
            AND ap._antibody_key = acc._object_key
            AND acc._mgitype_key = %(ANTIBODY_TYPEKEY)d
            AND acc._logicaldb_key = 1
            AND acc.preferred = 1
            AND acc.private = 0
            AND NOT a._assaytype_key IN (10, 11)
            ''')

        for r in self.context.sql(q):
            self.loadProbe(r['_assay_key'], r['accid'])
        return

    # Writes the whole record based on r and the key/value pairs in assay[][]
    def writeRecord(self, r):
        tmplt = '''
                <item class="GXDExpression" id="%(id)s" >
                  <reference name="publication" ref_id="%(publication)s" />
                  <attribute name="assayId" value="%(assayid)s" />
                  <attribute name="assayType" value="%(assaytype)s" />
                  <reference name="gene" ref_id="%(gene)s" />
                  <attribute name="sex" value="%(sex)s" />
                  <attribute name="age" value="%(age)s" />
                  <attribute name="ageMin" value="%(agemin)f" />
                  <attribute name="ageMax" value="%(agemax)f" />
                  <attribute name="strength" value="%(strength)s" />
                  <reference name="genotype" ref_id="%(genotype)s" />
                  <attribute name="theilerStage" value="%(stage)d" />
                  <reference name="structure" ref_id="%(structure)s" />
                  %(probe_wr)s
                  %(pattern_wr)s
                  %(image_wr)s
                 </item>
                 '''

        if r['_assay_key'] in self.assay:
            r['id'] = self.context.makeItemId('Expression')
            for k, v in self.assay[r['_assay_key']].items():
                r[k] = v

            #Add a temp variable: ['*_wr'] to eliminate nested attributes
            if 'probe' in r and len(r['probe']) > 1:
                r['probe_wr'] = '<attribute name="probe" value="%s" />' % r['probe']
            else:
                r['probe_wr'] = ''

            if 'pattern' in r and len(r['pattern']) > 1:
                r['pattern_wr'] = '<attribute name="pattern" value="%s" />' % r['pattern']
            else:
                r['pattern_wr'] = ''

            if 'image' in r and len(r['image']) > 1:
                r['image_wr'] = '<attribute name="imageFigure" value="%s" />' % r['image']
            else:
                r['image_wr'] = ''

            self.writeItem(r, tmplt)
            
            r['probe_wr'] = ''
            r['pattern_wr'] = ''
            r['image_wr'] = ''
        return

    
    # Pre-load the result (gellane/insitu) to structure relationship and include theilerstage
    def loadResultStructure(self, query):
        result2structure = defaultdict(list)

        for r in self.context.sql(query):
            result2structure[r['result_key']].append( (r['_structure_key'], r['stage']) )
        return result2structure


    # Pre-load the insitu result to image (mgi:id) relationship
    def loadResultImage(self):
        insitu2image = defaultdict(set)
        q = self.constructQuery('''
            SELECT isr._result_key, acc.accid
            FROM gxd_insituresultimage isr
            JOIN img_imagepane ip ON isr._imagepane_key = ip._imagepane_key
            JOIN acc_accession acc ON ip._image_key = acc._object_key
            JOIN img_image img ON ip._image_key = img._image_key
            WHERE acc._logicaldb_key = 1
            AND acc._mgitype_key = %(IMAGE_TYPEKEY)d
            AND acc.preferred = 1
            AND acc.private = 0
            AND img.xdim IS NOT NULL
            ''')

        for r in self.context.sql(q):
            insitu2image[r['_result_key']].add( r['accid'] )
        return insitu2image


    # Distill all gel bands strengths from a lane down to one value, based on Connie's rules:
    #  2,4,5,6,7,8 ==> Present, Present trumps Not Specified, Not Specified trumps Absent, Not Specified replaces Ambiguous
    def aggregateGelBands(self, gelbandStrengthsInLane):
        laneStrengthAgg = {}
        for (lane, gelbandStrengths) in gelbandStrengthsInLane.items():
            strengthAgg = None
            if (max(gelbandStrengths) > 3) or (2 in gelbandStrengths):
                strengthAgg = 'Present'
            elif (-1 in gelbandStrengths) or (3 in gelbandStrengths):
                strengthAgg = 'Not Specified'
            elif (1 in gelbandStrengths):
                strengthAgg = 'Absent'

            if strengthAgg is not None:
                laneStrengthAgg[lane] = strengthAgg
            else:
                print("ERROR: aggregateGelBands - Lane: ", lane, " has no gel bands.") 

        return laneStrengthAgg


    # Collect all gel band strengths for each gel lane, ignoring 'Not Applicable' (-2)
    def loadGelBandStrengthAggregate(self):
        gelbandsInLane = defaultdict(set)
        q = '''
            SELECT _gellane_key, _strength_key
            FROM gxd_gelband
            WHERE _strength_key > -2
            '''

        for r in self.context.sql(q):
            gelbandsInLane[r['_gellane_key']].add(r['_strength_key'])
        
        return self.aggregateGelBands(gelbandsInLane)


    # Write a record: foreach gellane (foreach gellane.structure))
    #   _gelcontrol_key: data lane = 1, control lane > 1 
    def processGelLane(self):
        gl2strength = self.loadGelBandStrengthAggregate()

        q = '''
            SELECT gl._gellane_key, gl._assay_key, gl.sex, gl.age, gl.agemin, gl.agemax, gl._genotype_key, gls._structure_key, ts.stage
            FROM gxd_gellane gl, gxd_gellanestructure gls, gxd_structure s, gxd_theilerstage ts
            WHERE gl._gellane_key = gls._gellane_key
            AND gls._structure_key = s._structure_key
            AND s._stage_key = ts._stage_key
            AND gl._gelcontrol_key = 1
            '''
        
        for r in self.context.sql(q):
            if r['_gellane_key'] in gl2strength:
                r['strength'] = gl2strength[r['_gellane_key']]
                r['genotype'] = self.context.makeItemRef('Genotype', r['_genotype_key'])
                r['structure'] = self.context.makeItemRef('EMAPXTerm', r['_structure_key'])
                self.writeRecord(r)
        return


    # Write a record: foreach specimen (foreach insituResult (foreach insitu.structure (foreach insitu.image)))
    def processInSitu(self):
        insitu2structureQuery = '''
            SELECT isrs._result_key AS result_key, isrs._structure_key, ts.stage
            FROM gxd_isresultstructure isrs, gxd_structure s, gxd_theilerstage ts
            WHERE isrs._structure_key = s._structure_key
            AND s._stage_key = ts._stage_key
            '''
        is2structure = self.loadResultStructure(insitu2structureQuery)
        is2image = self.loadResultImage()

        q = '''
            SELECT s._assay_key, s.sex, s.age, s.agemin, s.agemax, s._genotype_key, isr._result_key, str.strength, p.pattern
            FROM gxd_specimen s, gxd_insituresult isr, gxd_strength str, gxd_pattern p
            WHERE s._specimen_key = isr._specimen_key
            AND isr._strength_key = str._strength_key
            AND isr._pattern_key = p._pattern_key
            '''

        for r in self.context.sql(q):
            for (structure_key, theilerstage) in is2structure[r['_result_key']]:
                r['genotype'] = self.context.makeItemRef('Genotype', r['_genotype_key'])
                r['structure'] = self.context.makeItemRef('EMAPXTerm', structure_key)
                r['stage'] = theilerstage
                
                if r['_result_key'] in is2image:
                    for image in is2image[r['_result_key']]:
                        r['image'] = image
                        self.writeRecord(r)
                else:
                    self.writeRecord(r)
        return


    # Maps MGI:# to EMAP:# or MA:# from obo file
    def loadEMAPXMappings(self, file):
        def stanzaProc( stype, slines ):
            emapxId = None
            for tag, val in slines:
                if tag == "id" and (val.startswith("EMAP") or val.startswith("MA")):
                    emapxId = val
                    self.emapxList.append(val)
                elif tag == "alt_id" and val.startswith("MGI") and (emapxId is not None):
                    self.emapxRemap[val] = emapxId
        OboParser(stanzaProc).parseFile(file)


    # write out the EMAPX terms first, then GXD Expression terms can reference them
    def writeEMAPXTerms(self):
        self.emapxList = list()
        self.emapxRemap = {}
        if hasattr(self.context, 'emapxfile'):
            mfile = os.path.abspath(os.path.join(os.getcwd(),self.context.emapxfile))
            self.loadEMAPXMappings(mfile)
        
        tmplt = '''
                <item class="EMAPXTerm" id="%(id)s" >
                  <attribute name="identifier" value="%(identifier)s" />
                </item>
                '''

        q = self.constructQuery('''
            SELECT s._structure_key, s.edinburghkey, acc.accid
            FROM gxd_structure s, acc_accession acc
            WHERE s._structure_key = acc._object_key
            AND acc._mgitype_key = 38
            AND acc._logicaldb_key = 1
            AND acc.preferred = 1
            AND acc.private = 0
            ''')

        for r in self.context.sql(q):
            emapKey = "EMAP:" + str(r['edinburghkey'])
            if emapKey in self.emapxList:
                # EMAP term is current and used in obo file
                r['identifier'] = emapKey
            elif r['accid'] in self.emapxRemap: 
                # MGI id maps to an EMAP term or an MA term
                r['identifier'] = self.emapxRemap[r['accid']]
            else:
                # No EMAP or MA reference, use the MGI id
                r['identifier'] = r['accid']

            r['id'] = self.context.makeItemId('EMAPXTerm', r['_structure_key'])
            self.writeItem(r, tmplt)
        return


    def preDump(self):
        self.writeEMAPXTerms()
        self.assay = defaultdict(dict)

        self.loadAssay()
        self.loadProbePrep()
        self.loadAntibodyPrep()

        self.processGelLane()
        self.processInSitu()

        return


    def processRecord(self):
        return

