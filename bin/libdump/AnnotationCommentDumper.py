from AbstractItemDumper import *
from libdump import NoteUtils


class AnnotationCommentDumper(AbstractItemDumper):
  ITMPLT = '''
     <item class="Comment" id="%(id)s">
       <attribute name="type" value="%(type)s" />
       <attribute name="description" value="%(note)s" />
     </item>
     '''

  recordCount = 0

  def mainDump(self):
      for n in NoteUtils.iterNotes(_notetype_key=1008, _mgitype_key=25):
          n['type'] = 'MGI:General'
          self.preProcess(n)

      for n in NoteUtils.iterNotes(_notetype_key=1015, _mgitype_key=25):
          n['type'] = 'MGI:Background sensitivity'
          self.preProcess(n)

      for n in NoteUtils.iterNotes(_notetype_key=1031, _mgitype_key=25):
          n['type'] = 'MGI:Normal'
          self.preProcess(n)

  def preProcess(self, n):
          n['id'] = self.context.makeItemId('Comment',n['_note_key'])
          n['note'] = self.quote(n['note'])
          self.context.annotationComments.setdefault(n['_object_key'],[]).append('<reference ref_id="%s"/>'%n['id'])
          self._processRecord(n)
