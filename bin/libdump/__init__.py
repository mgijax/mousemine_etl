from DumperContext	import DumperContext

from AlleleDumper	import AlleleDumper
from AnnotationDumper	import AnnotationDumper
from ChromosomeDumper	import ChromosomeDumper
from CrossReferenceDumper import CrossReferenceDumper
from DataSourceDumper	import DataSourceDumper
from FeatureDumper	import FeatureDumper, MouseFeatureDumper, NonMouseFeatureDumper
from GenotypeDumper	import GenotypeDumper
from LocationDumper	import LocationDumper
from OrganismDumper	import OrganismDumper
from OrthologyDumper	import OrthologyDumper
from PublicationDumper	import PublicationDumper
from StrainDumper	import StrainDumper
from SynonymDumper	import SynonymDumper

def installMethods(module):
    import types
    for srcn, srcx in module.__dict__.items():
	tgtx = globals().get(srcn, None)
	if type(tgtx) is types.ClassType and type(srcx) is types.ClassType:
	    for sn, sx in srcx.__dict__.items():
		if type(sx) is types.FunctionType:
		    setattr(tgtx, sn, sx)
		    print "Installed: %s into %s"%(sn,tgtx)

