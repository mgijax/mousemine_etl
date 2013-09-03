from AbstractItemDumper import *
from collections import defaultdict 

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
            self.assay[r['_assay_key']]['gene'] = self.context.makeItemRef('Marker', r['_marker_key'])
            self.assay[r['_assay_key']]['publication'] = self.context.makeItemRef('Reference', r['_refs_key'])
            self.assay[r['_assay_key']]['assayid'] = r['accid']
            self.assay[r['_assay_key']]['assaytype'] = r['assaytype']

            if r['image'] is not None:
                self.assay[r['_assay_key']]['image'] = r['image'] 
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
                <item class="Expression" id="%(id)s" >
                  <reference name="gene" ref_id="%(gene)s" />
                  <reference name="publication" ref_id="%(publication)s" />
                  <attribute name="assayId" value="%(assayid)s" />
                  <attribute name="assayType" value="%(assaytype)s" />
                  <attribute name="sex" value="%(sex)s" />
                  <attribute name="age" value="%(age)s" />
                  <attribute name="ageMin" value="%(agemin)f" />
                  <attribute name="ageMax" value="%(agemax)f" />
                  <attribute name="strength" value="%(strength)s" />
                  <reference name="genotype" ref_id="%(genotype)s" />
                  <attribute name="theilerStage" value="%(stage)d" />
                  <reference name="structure" ref_id=%(structure)s" />
                  %(probe_wi)s
                  %(pattern_wi)s
                  %(image_wi)s
                 </item>
                 '''

        if r['_assay_key'] in self.assay:
            r['id'] = self.context.makeItemId('Expression')
            for k, v in self.assay[r['_assay_key']].items():
                r[k] = v

            #Add a temp variable: ['*_wi'] to eliminate nested attributes
            if 'probe' in r and len(r['probe']) > 1:
                r['probe_wi'] = '<attribute name="probe" value="%s" />' % r['probe']
            else:
                r['probe_wi'] = ''

            if 'pattern' in r and len(r['pattern']) > 1:
                r['pattern_wi'] = '<attribute name="pattern" value="%s" />' % r['pattern']
            else:
                r['pattern_wi'] = ''

            if 'image' in r and len(r['image']) > 1:
                r['image_wi'] = '<attribute name="imageFigure" value="%s" />' % r['image']
            else:
                r['image_wi'] = ''

            self.writeItem(r, tmplt)
            
            r['probe_wi'] = ''
            r['pattern_wi'] = ''
            r['image_wi'] = ''
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
            WHERE acc._logicaldb_key = 1
            AND acc._mgitype_key = %(IMAGE_TYPEKEY)d
            AND acc.preferred = 1
            AND acc.private = 0
            ''')

        for r in self.context.sql(q):
            insitu2image[r['_result_key']].add( r['accid'] )
        return insitu2image




    # Write a record: foreach gellane (foreach gelband (foreach gellane.structure))
    def processGelLane(self):
        gellane2structureQuery = '''
            SELECT gls._gellane_key AS result_key, gls._structure_key, ts.stage
            FROM gxd_gellanestructure gls, gxd_structure s, gxd_theilerstage ts
            WHERE gls._structure_key = s._structure_key
            AND s._stage_key = ts._stage_key
            '''
        gl2s = self.loadResultStructure(gellane2structureQuery)

        q = '''
            SELECT gl._assay_key, gl.sex, gl.age, gl.agemin, gl.agemax, gl._genotype_key, gb._gellane_key, s.strength
            FROM gxd_gellane gl, gxd_gelband gb, gxd_strength s
            WHERE gl._gellane_key = gb._gellane_key
            AND gb._strength_key = s._strength_key
            '''
        
        for r in self.context.sql(q):
            for (structure_key, theilerstage) in gl2s[r['_gellane_key']]:
                r['genotype'] = self.context.makeItemRef('Genotype', r['_genotype_key'])
                r['structure'] = self.context.makeItemRef('EMAPXTerm', structure_key)
                r['stage'] = theilerstage
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


    def writeEMAPXTerms(self):
        tmplt = '''
                <item class="EMAPXTerm" id="%(id)s" >
                  <attribute name="identifier" value="%(accid)s" />
                 </item>
                 '''

        q = self.constructQuery('''
            SELECT s._structure_key, acc.accid
            FROM gxd_structure s, acc_accession acc
            WHERE s._structure_key = acc._object_key
            AND acc._mgitype_key = 38
            AND acc._logicaldb_key = 1
            AND acc.preferred = 1
            AND acc.private = 0
            ''')

        for r in self.context.sql(q):
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

