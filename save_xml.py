from xml.dom import minidom
xmldoc = minidom.parse('output.xml')
dictionary = {}
lemma = xmldoc.getElementsByTagName('lemma')

for s in lemma:
	print s.attributes['name'].value
	y =  s.getElementsByTagName('document')
	for t in y:
		print t.attributes['id'].value
		print t.attributes['weight'].value
		if s.attributes['name'].value in dictionary:
			dictionary[s.attributes['name'].value].append([t.attributes['id'].value,t.attributes['weight'].value])
		else:
			dictionary[s.attributes['name'].value]=[[t.attributes['id'].value,t.attributes['weight'].value]]
