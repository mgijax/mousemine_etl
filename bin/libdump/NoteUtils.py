#
# NoteUtils.py
#
# Utility functions for dealing with MGI_Notes.
# The main entry point it the iterator, iterNotes. It allows you to easily iterate over
# the notes for an object, and type, or whatever (see below). But the main thing it does is hide
# hide the fact that in MGI, notes are broken up into 255 character chunks. iterNotes takes care
# of the annoying business of concatenating the chunks.
# It takes up to three keyword args:
#    _object_key = key of the objects you want notes for (default=all objects)
#		   (you should always combine with _mgitype_key)
#    _mgitype_key - key of the MGI type (ACC_MGIType) you want notes for
#    _notetype_key - key of the notetype (MGI_NoteType) you want.
#
# Example: iterate over the "General" notes for allele key = 138:
#	for n in libdump.NoteUtils.iterNotes(_object_key=138, _notetype_key=1020):
#		print n['note']
#

import mgidbconnect as db


def iterNotes( **kwargs ):
	note = None# current note to yield
	qry = buildQuery( ** kwargs )
	db.setConnectionFromPropertiesFile()
	notechunks = db.sql( qry )
	for nc in notechunks:
	    if nc['sequencenum'] == 1:
	        if note:
		    note['note'] = note['note'].strip()
		    yield note
		note = nc
	    else:
		note['note'] += nc['note']
		note['sequencenum'] = nc['sequencenum']
	if note:
	    note['note'] = note['note'].strip()
	    yield note

def buildQuery( _object_key = None, _notetype_key = None, _mgitype_key=None ):
        QTMPLT = '''
	SELECT n._note_key, n._notetype_key, n._object_key, n._mgitype_key, nc.sequencenum, nc.note
	FROM MGI_Note n, MGI_NoteChunk nc
	WHERE %s
	ORDER BY n._object_key, n._notetype_key, n._note_key, nc.sequenceNum
        '''
	whereParts = [ 
	    "n._note_key = nc._note_key"
	]
	if _notetype_key:
	    whereParts.append("n._notetype_key = %s" % _notetype_key)
	if _mgitype_key:
	    whereParts.append("n._mgitype_key = %s" % _mgitype_key)
	if _object_key:
	    whereParts.append("n._object_key = %s" % _object_key)
	whereClause = " AND ".join(whereParts)
	return QTMPLT % whereClause

def __test__():
	# print all notes for allele 138
	for n in iterNotes(_object_key = 138, _mgitype_key = 11):
	    print n['_note_key'], n['note']
	    print n

if __name__ == "__main__":
    __test__()
