from .AbstractItemDumper import *

class OrganismDumper(AbstractItemDumper):
    # This dumper used to query the MGI_Organism, 
    # Now it just dumps records based on config. See DumperContext.py
    QTMPLT = []
    ITMPLT = '''
    <item class="Organism" id="%(id)s">
       <attribute name="taxonId" value="%(taxon)s" />
       </item>
    '''
    def preDump(self):
        for r in list(self.context.QUERYPARAMS['ORGANISMS'].values()):
            oid = self.context.makeItemId('Organism', r[0])
            #self.context.log(str(r))
            #self.context.log(oid)
            self.writeItem({'id':oid, 'taxon':r[2]})
