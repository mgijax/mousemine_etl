
class PublicationDumper:
    def filter(self, r, s):
	return r.get('pubmedid',None) in ['19033647','17085523','21068358','1712752','8679216']

class StrainDumper:
    def filter(self, r, s):
        return r['_strain_key'] in [
	    27698, 33785, 33948, 34135, 34315, 34503, 35239, 36012, 38456, 38498,
	    38885, 38914, 39013, 39182, 39221, 40571, 43688, 43690, 43691, 43694,
	    47024, 47025, 285, 28319, 29662, 31421, 31848, 38872, 40754, 43565, ]

class MouseFeatureDumper:
    def filter(self, r, s):
	return r['_marker_key'] in [964]

class NonMouseFeatureDumper:
    def filter(self, r, s):
	return r['_marker_key'] in [43653, 19060]

class AlleleDumper:
    def filter(self, r, s):
	return r['_marker_key'] is not None


