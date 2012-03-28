from AbstractItemDumper import *


class StrainDumper(AbstractItemDumper):
    QTMPLT='''
    SELECT s._strain_key, s.strain AS name, t.term AS straintype, s.standard
    FROM PRB_Strain s, VOC_Term t
    WHERE s._straintype_key = t._term_key
    %(LIMIT_CLAUSE)s
    '''
    ITMPLT = '''
    <item class="Strain" id="%(id)s" >
      <reference name="organism" ref_id="%(organism)s" />
      <attribute name="symbol" value="%(symbol)s" />
      <attribute name="name" value="%(name)s" />
      <attribute name="strainType" value="%(straintype)s" />
      </item>
    '''
    def processRecord(self, r):
	r['id'] = self.context.makeItemId('Strain', r['_strain_key'])
	r['organism'] = self.context.makeItemRef('Organism', 1) # mouse
	r['name'] = self.quote(r['name'])
	r['symbol'] = r['name']
	r['straintype'] = self.quote(r['straintype'])
        return r

