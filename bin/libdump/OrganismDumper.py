from .AbstractItemDumper import *

class OrganismDumper(AbstractItemDumper):
    # This dumper used to query the MGI_Organism, 
    # Now it just dumps records based on config. See DumperContext.py
    QTMPLT = '''
    SELECT _organism_key, commonname, latinname
    FROM MGI_Organism 
    ORDER BY _organism_key
    '''
    ITMPLT = '''
    <item class="Organism" id="%(id)s">
       <attribute name="commonName" value="%(commonname)s" />
       <attribute name="name" value="%(latinname)s" />
       %(taxon)s
       %(genus)s
       %(species)s
       %(shortName)s
       </item>
    '''
    def processRecord (self, r) :
            r['id'] = self.context.makeItemId('Organism', r['_organism_key'])
            if r['commonname'] in ['Not Applicable', 'Not Loaded']:
                return None
            r['genus'] = ''
            r['species'] = ''
            r['shortName'] = ''
            r['taxon'] = ''
            if r['latinname'] not in ['Not Specified', 'wild-caught mouse', 'Mus interspecies crosses']:
                genus = r['latinname'].split()[0]
                species = r['latinname'].split(None, 1)[-1]
                r['genus'] = '<attribute name="genus" value="%s" />' % genus
                r['species'] = '<attribute name="species" value="%s" />' % species
                r['shortName'] = '<attribute name="shortName" value="%s" />' % (genus[0] + '. ' + species)
            r['taxon'] = ""
            taxon = self.context.QUERYPARAMS['ORGANISMKEY2TAXON'].get(r['_organism_key'], None)
            if taxon:
                r['taxon'] = '<attribute name="taxonId" value="%s" />' % taxon
            return r
