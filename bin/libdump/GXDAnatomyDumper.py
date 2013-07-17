#--------------------------------------------
# Given an EMAP and MA file this will merge them into a single ontology.
# All MA terms have TS28 added to their names.
# MA:0000001 is made obsolete and all is immediate children are
# made children of EMAP:0.
# All MGI ID's from GXD are incorporated into the ontology
# Where possible a GXD anatomy term's MGI ID is added to the corresponding EMAP/MA term
# otherwise a new term is created.
# GXD conceptus terms are explicity mapped the the correct EMAP term.
# A hard coded list of GDX terms are mapped to EMAP terms with slightly different names





from OboParser import OboParser, formatStanza
from collections import defaultdict
import os
import sys
import mgiadhoc as db

class GXDAnatomyDumper:

    def main(self, maFile, emapFile, emapxFile):
        tempFile = "./temp.obo"
        self.term_stanzas = {}
        self.other_stanzas = []
        self.mgi2emap = {}
        self.termmap = {}
        self.mergeEMAPandMA(maFile, emapFile)
        self.loadEMAP(emapFile)
        self.loadGXD(tempFile)
        self.fixMGITerms(tempFile,emapxFile)
 
    def mergeEMAPandMA(self, maFile, emapFile):
        fd = open(emapFile,'a')
        def stanzaProc(stype,slines):
            if (stype == "Term"):
                for i,line in enumerate(slines):
                    tag, val = line
                    if (tag == "name"):
                        slines[i] = (tag, "TS28 "+val)
                    if((tag =='is_a') and (val[:val.find('!')].strip()=='MA:0000001')):
                        slines[i] = (tag, "EMAP:0 ! stage specific anatomical structure")
                    if((tag =='id') and (val == 'MA:0000001')):
			slines.append(("is_obsolete", "true"))
                fd.write(formatStanza(stype,slines))
                fd.write("\n")
        OboParser(stanzaProc).parseFile(maFile)
        fd.close() 


    def loadEMAP(self, emapFile):
         def stanzaLoader(stype,slines):
             if (stype == "Term"):
                 for i, line in enumerate(slines):
                     tag, val = line
                     if (tag=="id"):
                         self.term_stanzas[str(val)]=[stype,slines]
             else:
                 self.other_stanzas.append((stype, slines))
         OboParser(stanzaLoader).parseFile(emapFile)

    def loadGXD(self,tempFile):
        fd = open(tempFile,'w')
        for x in self.other_stanzas:
            fd.write(formatStanza(x[0],x[1]))
            fd.write("\n")
        sqlGXD = '''
    SELECT
       s._structure_key,
       a2.accID as mgi_id,
       'EMAP:'|| s.edinburghKey as edinburghkey,
       a.accID as ma_key,
       s.printName,
       s._stage_key,
       s._parent_key,
       s2.printName as parentname,
       'EMAP:'|| s2.edinburghKey as parentEdinburgh,
       a3.accID as parentMGI,
       a4.accID as parentMA
    FROM
       GXD_Structure s 
       LEFT OUTER JOIN ACC_ACCESSION a on (s._structure_key = a._object_key and a._logicaldb_key = 42 )
       LEFT OUTER JOIN ACC_ACCESSION a2 on (s._structure_key =
           a2._object_key and a2._logicaldb_key = 1 and a2._mgitype_key = 38)
       LEFT OUTER JOIN ACC_ACCESSION a3 on (s._parent_key =
           a3._object_key and a3._logicaldb_key = 1 and a3._mgitype_key = 38)
       LEFT OUTER JOIN ACC_ACCESSION a4 on (s._parent_key = a4._object_key and a4._logicaldb_key = 42)
       LEFT OUTER JOIN GXD_STRUCTURE s2 on (s._parent_key = s2._structure_key)
    WHERE
       s._structure_key != 6936
        '''


        Template='''
[Term]
id: {0}
name: TS{1} {2}
relationship: part_of {3} ! {4}

'''
        mgi_2_ma = 0
        mgi_2_emap = 0
        new_mgi = 0
        ma_emap = 0
        self.addMGI2EMAP()
        for r in db.sql(sqlGXD):
            mgi_id = r['mgi_id']
            ed_key = (r['edinburghkey'])
            ma_key = (r['ma_key'])
            stage_key = r['_stage_key']
            if(stage_key < 10):
             stage_key = "0"+str(stage_key)
            found = False
            if(mgi_id in self.mgi2emap):
                ed_key = self.mgi2emap[mgi_id]
            if (ma_key in self.term_stanzas):
                self.term_stanzas[ma_key][1].append(("alt_id",mgi_id))
                found = True
                mgi_2_ma = mgi_2_ma + 1
            if (ed_key in self.term_stanzas):
                self.term_stanzas[ed_key][1].append(("alt_id",mgi_id))
                found = True
                mgi_2_emap = mgi_2_emap + 1
            if(not found):
                for x in self.term_stanzas.values():
                    for i, line in enumerate(x[1]):
                     tag, val = line
                     if (tag=="name"):
                         fullname = "TS{0} {1}".format(stage_key,r['printname'])
        #                 print("comparing {0} to {1}".format(val,fullname))
                         if( val == fullname):
                             x[1].append(("alt_id",mgi_id))
      #                      print("added {0} to {1} based on name".format(mgi_id,val))
                             if(stage_key == 28):
                                 mgi_2_ma = mgi_2_ma + 1
                             else:
                                 mgi_2_emap = mgi_2_emap + 1
                             found = True 
            if(not found):
                parent_key = r['parentmgi']
                if(r['parentedinburgh'] is not None):
                    parent_key = r['parentedinburgh']
                if(r['parentma'] is not None):
                    parent_key = r['parentma']
                if(parent_key not in self.term_stanzas):
                    parent_key = r['parentmgi']
                fd.write(Template.format(mgi_id,stage_key,r['printname'],parent_key,r['parentname']))
                new_mgi = new_mgi + 1
        for x in self.term_stanzas.values():
            fd.write(formatStanza(x[0],x[1]))
            fd.write("\n")
            ma_emap = ma_emap + 1
        fd.close()
        print('wrote {0} EMAP/MA terms, mapped {1} mgi terms to EMAP, {2} terms to MA, created {3} new mgi terms'.format(str(ma_emap),str(mgi_2_emap),str(mgi_2_ma),str(new_mgi)))


    def fixMGITerms(self,tempFile,emapxFile):
         def termLoader(stype,slines):
             if (stype == "Term"):
                 mgi_id = ""
                 term_id = ""
                 for i, line in enumerate(slines):
                     tag, val = line
                     if (tag=="id"):
                         term_id = val
                     if (tag=="alt_id"):
                         mgi_id = val
                         self.termmap[mgi_id] = term_id
         OboParser(termLoader).parseFile(tempFile)
         self.addConceptus()
         fd = open(emapxFile,'w')
         def termReMapper(stype,slines):
             if(stype=="Term"):
                 for i, line in enumerate(slines):
                     tag, val = line
                     if (tag=="relationship"):
                         id = val[8:val.find('!')].strip()
                         if(id in self.termmap):
                             slines[i] = (tag, 'part_of '+ self.termmap[id])
                     if (tag=="id"):
                         if(val in self.conceptus):
                             for j,line in enumerate(slines):
                                 tag2, val2 = line
                                 if(tag2=="relationship"): 
                                     slines[j] = (tag2, 'part_of '+self.conceptus[val])
                            
             fd.write(formatStanza(stype,slines))
             fd.write("\n")
         OboParser(termReMapper).parseFile(tempFile)
         os.remove(tempFile)

    def addConceptus(self):
        self.conceptus = {'MGI:4850277':'EMAP:25766', 'MGI:4850282':'EMAP:25767', 'MGI:4850286':'EMAP:25768', 'MGI:4850291':'EMAP:25769', 'MGI:4850303':'EMAP:25770', 'MGI:4850312':'EMAP:25771', 'MGI:4850324':'EMAP:25772', 'MGI:4850340':'EMAP:25773', 'MGI:4850364':'EMAP:25774', 'MGI:4850391':'EMAP:25775', 'MGI:4850433':'EMAP:25776', 'MGI:4850494':'EMAP:25777', 'MGI:4850690':'EMAP:25778', 'MGI:4850929':'EMAP:25779', 'MGI:4851289':'EMAP:25780', 'MGI:4851750':'EMAP:25781', 'MGI:4852296':'EMAP:25782', 'MGI:4852927':'EMAP:25783', 'MGI:4853613':'EMAP:25784', 'MGI:4854267':'EMAP:25785', 'MGI:4855024':'EMAP:25786', 'MGI:4855982':'EMAP:25787', 'MGI:4857139':'EMAP:25788', 'MGI:4857142':'EMAP:25789', 'MGI:4857145':'EMAP:25790', 'MGI:4857148':'EMAP:25791', 'MGI:4857151':'EMAP:30155'}

    def addMGI2EMAP(self):
        self.mgi2emap = {'MGI:5287706':'EMAP:14883', 'MGI:5287707':'EMAP:14884', 'MGI:5287708':'EMAP:14885', 'MGI:5004690':'EMAP:29493', 'MGI:5004696':'EMAP:29594', 'MGI:5004698':'EMAP:31794', 'MGI:5003549':'EMAP:29779', 'MGI:5003535':'EMAP:31800', 'MGI:5003537':'EMAP:29363', 'MGI:5003517':'EMAP:30859', 'MGI:5003306':'EMAP:29982', 'MGI:5003303':'EMAP:30042', 'MGI:5003294':'EMAP:29521', 'MGI:5003295':'EMAP:31311', 'MGI:5003297':'EMAP:29550', 'MGI:5003260':'EMAP:31444', 'MGI:5003218':'EMAP:30519', 'MGI:5003103':'EMAP:28815', 'MGI:5003106':'EMAP:30854', 'MGI:5003077':'EMAP:28143', 'MGI:5003078':'EMAP:28148', 'MGI:5003079':'EMAP:28153', 'MGI:5003080':'EMAP:30038', 'MGI:5003083':'EMAP:28158', 'MGI:5003085':'EMAP:28163', 'MGI:5003087':'EMAP:28168', 'MGI:5003089':'EMAP:30044', 'MGI:5000075':'EMAP:30856'}
        self.mgi2emap.update({'MGI:5000061':'EMAP:28338', 'MGI:5000062':'EMAP:28347', 'MGI:5000067':'EMAP:28365', 'MGI:5000068':'EMAP:28371', 'MGI:5000069':'EMAP:28160', 'MGI:5000050':'EMAP:28140', 'MGI:5000051':'EMAP:28145', 'MGI:5000029':'EMAP:27959', 'MGI:5000018':'EMAP:28522', 'MGI:4999934':'EMAP:28915', 'MGI:4999860':'EMAP:28557', 'MGI:4999861':'EMAP:28563', 'MGI:4999862':'EMAP:28568', 'MGI:4999863':'EMAP:28571', 'MGI:4999864':'EMAP:30852', 'MGI:4999596':'EMAP:27578', 'MGI:4867665':'EMAP:28559', 'MGI:4866918':'EMAP:28520', 'MGI:4866919':'EMAP:28138', 'MGI:4866898':'EMAP:28812', 'MGI:4866890':'EMAP:28918', 'MGI:4866886':'EMAP:28916', 'MGI:4866791':'EMAP:32025', 'MGI:4866784':'EMAP:32024', 'MGI:4866653':'EMAP:31178', 'MGI:4866654':'EMAP:31116', 'MGI:4866657':'EMAP:31179', 'MGI:4866658':'EMAP:31117'})
        self.mgi2emap.update({'MGI:4866661':'EMAP:31180', 'MGI:4866597':'EMAP:33903', 'MGI:4866571':'EMAP:31165', 'MGI:4866525':'EMAP:32026', 'MGI:4866496':'EMAP:34624', 'MGI:4866497':'EMAP:34105', 'MGI:4866486':'EMAP:34103', 'MGI:4866466':'EMAP:34925', 'MGI:4866469':'EMAP:34926', 'MGI:4866472':'EMAP:34896', 'MGI:4866475':'EMAP:34897', 'MGI:4866478':'EMAP:34898', 'MGI:4866481':'EMAP:34899', 'MGI:4866423':'EMAP:33904', 'MGI:4866417':'EMAP:33408', 'MGI:4866388':'EMAP:34626', 'MGI:4866337':'EMAP:31163', 'MGI:4866330':'EMAP:33409', 'MGI:4866307':'EMAP:26223', 'MGI:4865996':'EMAP:31170', 'MGI:4865998':'EMAP:31174', 'MGI:4866000':'EMAP:31175', 'MGI:4866002':'EMAP:31176', 'MGI:4866004':'EMAP:31177', 'MGI:4865984':'EMAP:34625', 'MGI:4865975':'EMAP:34627', 'MGI:4865965':'EMAP:34622', 'MGI:4865966':'EMAP:34623', 'MGI:4865814':'EMAP:31173', 'MGI:4865498':'EMAP:34934', 'MGI:4865436':'EMAP:33750', 'MGI:4865439':'EMAP:33751', 'MGI:4865442':'EMAP:33752', 'MGI:4865445':'EMAP:33753', 'MGI:4865448':'EMAP:33754', 'MGI:4865451':'EMAP:33755', 'MGI:4865454':'EMAP:33756', 'MGI:4865202':'EMAP:34359', 'MGI:4865203':'EMAP:34360', 'MGI:4865204':'EMAP:34364', 'MGI:4865205':'EMAP:34365', 'MGI:4865206':'EMAP:34366', 'MGI:4865207':'EMAP:34367', 'MGI:4865186':'EMAP:33894', 'MGI:4865113':'EMAP:31172', 'MGI:4865114':'EMAP:31110', 'MGI:4865115':'EMAP:31160', 'MGI:4865118':'EMAP:31171', 'MGI:4864959':'EMAP:34361', 'MGI:4864961':'EMAP:34363', 'MGI:4864964':'EMAP:34947', 'MGI:4864966':'EMAP:34362', 'MGI:4864973':'EMAP:34946', 'MGI:4864975':'EMAP:34948', 'MGI:4864977':'EMAP:34128', 'MGI:4864979':'EMAP:34129', 'MGI:4864981':'EMAP:34130', 'MGI:4864920':'EMAP:33462', 'MGI:4864925':'EMAP:33463', 'MGI:4864750':'EMAP:34463', 'MGI:4864752':'EMAP:34454', 'MGI:4864754':'EMAP:34455', 'MGI:4864756':'EMAP:34456', 'MGI:4864758':'EMAP:34457', 'MGI:4864760':'EMAP:34458', 'MGI:4864762':'EMAP:34459', 'MGI:4864764':'EMAP:34460', 'MGI:4864766':'EMAP:34461', 'MGI:4864768':'EMAP:34462', 'MGI:4864317':'EMAP:34313', 'MGI:4864320':'EMAP:34314', 'MGI:4864323':'EMAP:34315', 'MGI:4864326':'EMAP:34316', 'MGI:4864329':'EMAP:34317', 'MGI:4864332':'EMAP:34318', 'MGI:4864335':'EMAP:34319', 'MGI:4864338':'EMAP:34320', 'MGI:4864341':'EMAP:34321', 'MGI:4864344':'EMAP:34322', 'MGI:4864347':'EMAP:34323', 'MGI:4864350':'EMAP:34324'})

GXDAnatomyDumper().main(sys.argv[1], sys.argv[2], sys.argv[3]) 
