from AbstractItemDumper import *

class OrganismDumper(AbstractItemDumper):
    # This dumper used to query the MGI_Organism, 
    # Now it just dumps records based on config. See DumperContext.py
    QTMPLT = []
    ITMPLT = '''
    <item class="Organism" id="%(id)s">
       <attribute name="taxonId" value="%(accid)s" />
       </item>
    '''
    def preDump(self):
        for r in self.context.QUERYPARAMS['ORGANISMS'].values():
	    self.writeItem({'id':r[0], 'accid':r[2]})
