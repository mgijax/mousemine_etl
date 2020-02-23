from .DumperContext      import DumperContext

from .AlleleDumper       import AlleleDumper
from .AnnotationDumper   import AnnotationDumper
from .CellLineDumper     import CellLineDumper
from .ChromosomeDumper   import ChromosomeDumper
from .CrossReferenceDumper import CrossReferenceDumper
from .DataSourceDumper   import DataSourceDumper
from .ExpressionDumper   import ExpressionDumper
from .FeatureDumper      import FeatureDumper, MouseFeatureDumper, NonMouseFeatureDumper
from .GenotypeDumper     import GenotypeDumper
from .HomologyDumper     import HomologyDumper
from .LocationDumper     import LocationDumper
from .OboParser          import OboParser
from .OrganismDumper     import OrganismDumper
from .ProteinDumper      import ProteinDumper
from .PublicationDumper  import PublicationDumper
from .RelationshipDumper import RelationshipDumper
from .StrainDumper       import StrainDumper
from .SynonymDumper      import SynonymDumper
from .SyntenyDumper      import SyntenyDumper
from .AnnotationCommentDumper import AnnotationCommentDumper
from . import NoteUtils

def installMethods(module):
    import types
    for srcn, srcx in list(module.__dict__.items()):
        tgtx = globals().get(srcn, None)
        if type(tgtx) is type and type(srcx) is type:
            for sn, sx in list(srcx.__dict__.items()):
                if type(sx) is types.FunctionType:
                    setattr(tgtx, sn, sx)
                    print("Installed: %s into %s"%(sn,tgtx))

