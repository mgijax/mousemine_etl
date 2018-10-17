from AbstractItemDumper import *
from collections import defaultdict 
from OboParser import OboParser

class ExpressionDumper(AbstractItemDumper):

    # Pre-loads assay information.
    # The assay structure is a dict of dict.
    def loadAssay(self):
        q = self.constructQuery('''
            SELECT a._assay_key, a._marker_key, a._refs_key, acc.accid, at.assaytype, a.creation_date
            FROM gxd_assay a
              JOIN acc_accession acc 
                ON a._assay_key = acc._object_key 
              JOIN gxd_assaytype at 
                ON a._assaytype_key = at._assaytype_key
            WHERE acc._logicaldb_key = 1
            AND acc._mgitype_key = %(ASSAY_TYPEKEY)d
            AND acc.preferred = 1
            AND acc.private = 0
            AND NOT a._assaytype_key IN (10, 11)
            ''')

        for r in self.context.sql(q):
            ak = r['_assay_key']
            self.assay[ak]['feature'] = self.context.makeItemRef('Marker', r['_marker_key'])
            self.assay[ak]['publication'] = self.context.makeItemRef('Reference', r['_refs_key'])
            self.assay[ak]['assayid'] = r['accid']
            self.assay[ak]['assaytype'] = r['assaytype']
            self.assay[ak]['annotationdate'] = str(r['creation_date']).split()[0]

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
        attributeList = ("probe", "pattern", "image")
        noneTypeList = ("detected", "note")

        tmplt = '''
                <item class="GXDExpression" id="%(id)s" >
                  <reference name="publication" ref_id="%(publication)s" />
                  <attribute name="assayId" value="%(assayid)s" />
                  <attribute name="assayType" value="%(assaytype)s" />
                  <reference name="feature" ref_id="%(feature)s" />
                  <attribute name="sex" value="%(sex)s" />
                  <attribute name="age" value="%(age)s" />
                  <attribute name="strength" value="%(strength)s" />
                  <reference name="genotype" ref_id="%(genotype)s" />
                  <attribute name="stage" value="TS%(stage)02d" />
                  <attribute name="emaps" value="%(emaps)s" />
                  <reference name="structure" ref_id="%(structure)s" />
                  <attribute name="specimenNum" value="%(specimennum)i" />
                  %(probe_wv)s
                  %(pattern_wv)s
                  %(image_wv)s
                  %(detected_wv)s
                  %(note_wv)s
                  <attribute name="annotationDate" value="%(annotationdate)s" />
                 </item>
                 '''

        if r['_assay_key'] in self.assay:
            r['id'] = self.context.makeItemId('Expression')
            for k, v in self.assay[r['_assay_key']].items():
                r[k] = v

            for att in attributeList:
                att_wv = att + "_wv"   # _wv: write value
                if att in r and len(r[att]) > 0:
                    r[att_wv] = '<attribute name="{0}" value="{1}" />'.format(att, self.quote(r[att]))
                else:
                    r[att_wv] = ''                        

            for nt in noneTypeList:
                nt_wv = nt + "_wv"
                if nt in r and r[nt] is not None:
                    r[nt_wv] = '<attribute name="{0}" value="{1}" />'.format(nt, self.quote(r[nt]))
                else:
                    r[nt_wv] = ''

            self.writeItem(r, tmplt)

        return

    
    # Pre-load the set of insitu images result keys
    def loadImageResultKeys(self):
        insituImageResultKeys = set()
        q = self.constructQuery('''
            SELECT DISTINCT isr._result_key
            FROM gxd_insituresultimage isr
            JOIN img_imagepane ip ON isr._imagepane_key = ip._imagepane_key
            JOIN img_image img ON ip._image_key = img._image_key
            WHERE img.xdim IS NOT NULL
            ''')

        for r in self.context.sql(q):
            insituImageResultKeys.add(r['_result_key'])
        return insituImageResultKeys


    # Pre-load the set of assay image figure labels
    def loadAssayImageFigureLabels(self):
        assay2imageFigureLabel = dict()
        q = self.constructQuery('''
            SELECT DISTINCT a._assay_key, img.figurelabel
            FROM gxd_assay a
            JOIN img_imagepane ip ON a._imagepane_key = ip._imagepane_key
            JOIN img_image img ON ip._image_key = img._image_key
            WHERE a._imagepane_key IS NOT NULL 
            AND img.xdim IS NOT NULL
            AND img.figurelabel IS NOT NULL
            ''')

        for r in self.context.sql(q):
            assay2imageFigureLabel[r['_assay_key']] = r['figurelabel']
        return assay2imageFigureLabel


    # Distill all gel bands strengths from a lane down to one value, based on Connie's rules:
    #  2,4,5,6,7,8 ==> Present, Present trumps Not Specified, Not Specified trumps Absent, Not Specified replaces Ambiguous
    def aggregateGelBands(self, gelbandsInLane):
        lane2strength = {}
        for (lane, strengths) in gelbandsInLane.items():
            agg = None
            if (max(strengths) > 3) or (2 in strengths):
                agg = 'Present'
            elif (-1 in strengths) or (3 in strengths):
                agg = 'Not Specified'
            elif (1 in strengths):
                agg = 'Absent'

            if agg is not None:
                lane2strength[lane] = agg
            else:
                print("ERROR: aggregateGelBands - Lane: ", lane, " has no gel bands.") 

        return lane2strength


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
        ak2figurelabel = self.loadAssayImageFigureLabels()

        q = '''
            SELECT gl._gellane_key, gl._assay_key, gl.sex, gl.age, gl._genotype_key, gl.sequencenum as specimennum, a.accid AS emapa, gls._stage_key as stage
            FROM gxd_gellane gl, gxd_gellanestructure gls, acc_accession a
            WHERE gl._gellane_key = gls._gellane_key                                                            
            AND a._object_key = gls._emapa_term_key
            AND a._mgitype_key = 13
            AND a._logicaldb_key = 169
            AND a.preferred = 1
            AND a.private = 0
            AND gl._gelcontrol_key = 1
            '''

        for r in self.context.sql(q):
            if r['_gellane_key'] in gl2strength:
                r['strength'] = gl2strength[r['_gellane_key']]
                r['genotype'] = self.context.makeItemRef('Genotype', r['_genotype_key'])

                isDetected = self.strengthToBoolean(r['strength'])
                if isDetected is not None:
                    r['detected'] = isDetected

                ak = r['_assay_key']
                if r['_assay_key'] in ak2figurelabel:
                    r['image'] = ak2figurelabel[r['_assay_key']]
                    
                r['structure'] = self.context.makeItemRef('EMAPATerm', int(r['emapa'][-5:]))
                r['emaps'] = r['emapa'].replace('EMAPA','EMAPS') + str(r['stage'])                
                self.writeRecord(r)
        return


    # Convert strength to a boolean value
    def strengthToBoolean(self, strength):
        strengthLowerCase = strength.lower()
        if strengthLowerCase in ["very strong", "strong", "moderate", "weak", "trace", "present"]:
            return True
        elif strengthLowerCase in ["absent"]:
            return False
        elif strengthLowerCase in ["ambiguous", "not specified", "not applicable"]:
            return None
        else:
            print("Error - Expression Dumper:strengthToDetected: ", strength, " not found.")
            return None


    # Write a record: specimen X insituResult X insitu.structure
    def processInSitu(self):
        isImageResultKeys = self.loadImageResultKeys()

        q = '''
            SELECT s._assay_key, s.sex, s.age, s._genotype_key, s.sequencenum as specimennum, s.specimenlabel AS image, isr._result_key, isr.resultNote AS note, str.strength, p.pattern, irs._stage_key as stage, a.accid AS emapa
            FROM gxd_specimen s, gxd_insituresult isr, gxd_strength str, gxd_pattern p, gxd_isresultstructure irs, acc_accession a 
            WHERE s._specimen_key = isr._specimen_key
            AND isr._strength_key = str._strength_key
            AND isr._pattern_key = p._pattern_key
            AND isr._result_key = irs._result_key
            AND a._object_key = irs._emapa_term_key
            AND a._mgitype_key = 13
            AND a._logicaldb_key = 169
            AND a.preferred = 1
            AND a.private = 0
            '''


        for r in self.context.sql(q):
            r['genotype'] = self.context.makeItemRef('Genotype', r['_genotype_key'])
                
            isDetected = self.strengthToBoolean(r['strength'])
            if isDetected is not None:
                r['detected'] = isDetected

            if r['_result_key'] not in isImageResultKeys:
                r['image'] = ''
                
            r['structure'] = self.context.makeItemRef('EMAPATerm', int(r['emapa'][-5:]))
            r['emaps'] = r['emapa'].replace('EMAPA','EMAPS') + str(r['stage'])
            self.writeRecord(r)
        return


    # write out the EMAPA terms first, then GXD Expression terms can reference them
    def writeEMAPATerms(self):
        tmplt = '''
                <item class="EMAPATerm" id="%(id)s" >
                  <attribute name="identifier" value="%(identifier)s" />
                </item>
                '''

        q = self.constructQuery('''
            SELECT t.term, a.accid AS emapa
            FROM voc_term t, acc_accession a
            WHERE t._vocab_key = 90
            AND t._term_key = a._object_key
            AND a._mgitype_key = 13
            AND a.private = 0
            AND a.preferred = 1
            AND a._logicaldb_key = 169
	    AND t._term_key in (select _term_key from voc_term_emapa)
            ''')

        referenced_emapaids = set()

        for r in self.context.sql(q):
            if r['emapa'] not in referenced_emapaids:
                r['identifier'] = r['emapa']
                r['id'] = self.context.makeItemId('EMAPATerm', int(r['emapa'][-5:]))
                referenced_emapaids.add(r['emapa'])

                self.writeItem(r, tmplt)
        return



    def preDump(self):
        self.writeEMAPATerms()
        self.assay = defaultdict(dict)

        self.loadAssay()
        self.loadProbePrep()
        self.loadAntibodyPrep()

        self.processGelLane()
        self.processInSitu()

        return


    def processRecord(self):
        return

