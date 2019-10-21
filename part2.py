# -*- coding: utf-8 -*-
import json
import MySQLdb
import nltk
import base64
from collections import defaultdict
from math import log10
from nltk.stem.wordnet import WordNetLemmatizer
import xml.etree.cElementTree as ET
import xml.etree.ElementTree
from xml.dom import minidom
import os
import sys
import codecs
import string
import math
import io
from warnings import filterwarnings
filterwarnings('ignore', category = MySQLdb.Warning)
from collections import Counter
from lxml import etree
from io import open

db = MySQLdb.connect(host = "localhost",
					user = "root",
					passwd = "root",
					db = "Project_LP2",charset='utf8')

cur = db.cursor()

x = []
y = []
list_of_lists = []
list_of_words = []


def load_database():
	#Code to open /articles/*
	#path = "/home/andronikos/Downloads/20news-bydate/20news-bydate-train"
	path = "/home/andronikos/Downloads/20news-bydate/20news-bydate-train/selection"
	for folder in os.listdir(path):
		sys.stdout.write('\n')
		print "Inserting from: " + folder + "\n"
		num_of_mails = 0
		num_of_words = 0
		for file in os.listdir(path+"/"+folder):
			name = path +"/"+folder+"/"+file
			with codecs.open(name, 'r',encoding='utf-8', errors='ignore') as f:
				mail = f.read()
				#Insert Articles & Words in Database
				lmtzr = WordNetLemmatizer()
	 			url = file
				text = mail.decode('utf-8')
				text = string.replace(text,'/',' ')
				text = string.replace(text,'\\',' ')
				text = string.replace(text,'*',' ')
				text = string.replace(text,'@',' ')
				text = string.replace(text,'.',' ')
				text = string.replace(text,'=',' ')
				text = string.replace(text,'^',' ')
				text = string.replace(text,'%',' ')
				text = string.replace(text,',',' ')
				text = string.replace(text,'+',' ')
				text = string.replace(text,'_',' ')
				text = string.replace(text,"'",' ')
				text = string.replace(text,'-',' ')
				text = string.replace(text,'!',' ')
				text = text.lower()
				i=0;
				#Insert Article into Database
				cur.execute("INSERT IGNORE INTO Mails(URL,MainText,Category) VALUES (%s,%s,%s)",(url.encode('utf-8'),text.encode('utf-8'),folder.encode('utf-8')))
				db.commit()
				num_of_mails = num_of_mails + 1
				tokens = nltk.word_tokenize(text) #tokenize 
				tagged = nltk.pos_tag(tokens) 	   #tag each token
				#find postag
				for word in tokens:
					# word = word.lower()
					verb_tags = {"VB","VBD","VBG","VBN","VBP","VBZ"}
					if tagged[i][1] in verb_tags:
						lword = lmtzr.lemmatize(word,pos='v')
					else:
						lword = lmtzr.lemmatize(word)
					#choose if tag belongs in open or close list
					open_tags = {"JJ","JJR","JJS","RB","RBR","RBS","NN","NNS","NNP","NNPS","VB","VBD","VBG","VBN","VBP","VBZ","FW"}
					if tagged[i][1] in open_tags:
						category = 0
					else:
						category = 1
					# Insert Words of each Article into Database
					cur.execute("INSERT IGNORE INTO Words(ID,Word,Tag,Lemma,OpenClose) VALUES (%s,%s,%s,%s,%s)",(file,word.encode('utf-8'),tagged[i][1].encode('utf-8'),lword.encode('utf-8'),category))
					db.commit()
					i = i + 1
				num_of_words = num_of_words + i - 1 
	 			sys.stdout.write('\r')
	 			sys.stdout.write("Inserted Mails/Inserted Words: %d / %d" %(num_of_mails,num_of_words))
	 			sys.stdout.flush()

def calculate_tf_idf():
	# find number of mails stored in databse (N)
	cur.execute("SELECT COUNT(*) FROM Mails ORDER BY URL")
	N = cur.fetchone()
	#print "Number of Mails: ",N[0]
	limit = N[0]
	#Calculate Max Frequency of term in each Article
	tmp1 = {}
	weight = defaultdict(list)
	cur.execute("SELECT URL FROM Mails ORDER BY URL")
	sys.stdout.write('\n')
	counter = 0
	
	for id in cur.fetchall():
		counter = counter + 1
		percent = float(counter) / float(limit) *100
		sys.stdout.write('\r')
		sys.stdout.write("Processing Mail %d / Processed Mails %d - %f %%" %(id[0],counter,percent))
		sys.stdout.flush()
		#Calculate Max frequency of term in an article
		cur.execute("SELECT MAX(counted) FROM ( SELECT COUNT(*)AS counted FROM Words WHERE ID = '%d' AND OpenClose = 0 GROUP BY Lemma ) AS counts"%(id))
		maxf = cur.fetchone()[0] 
		# find tf of each term = term frequency / max frequency and insert to dictionary -- is /maxf needed???
		tmp2 = {}
		cur.execute("SELECT Lemma,COUNT(*) FROM Words WHERE ID = '%d' AND OpenClose = 0 GROUP BY Lemma"%(id))
		tmp2 = {lemma:[id[0],float(float(count/float(maxf)))] for (lemma,count) in cur.fetchall()}
		# tmp2 = {lemma:[id[0],count] for (lemma,count) in cur.fetchall()}
		# merge dictionaries together
		for d in (tmp1,tmp2): 
			for key, value in d.iteritems():
				weight[key].append(value)

	# find number of mails stored in databse (N)
	cur.execute("SELECT COUNT(*) FROM Mails ORDER BY URL")
	N = cur.fetchone()

	# find number of times a term is stored in database and calculate tf x idf
	cur.execute("SELECT Lemma,COUNT(DISTINCT ID) FROM Words WHERE OpenClose = 0 GROUP BY Lemma")
	for lemma,n in cur.fetchall():
		for i in weight[lemma]:
			#calculate tf x idf
			i[1] = float(i[1]) * float(log10(float(N[0])/float(n))) 
	return weight

def export_to_xml(weight):
	sys.stdout.write('\n')
	sys.stdout.write('\r')
	sys.stdout.write("Exporting to XML")
	sys.stdout.flush()
	#create xml file
	keylist = weight.keys()
	keylist.sort()
	root = ET.Element("inverted_index")
	for lemma in keylist:
		xml_lemma = ET.SubElement(root,"lemma",name = lemma)
		for i in weight[lemma]:
			ET.SubElement(xml_lemma,"document",id = str(i[0]), weight = str(i[1]))
	tree = ET.ElementTree(root)

	# with codecs.open("yop", "w", encoding="utf-8") as f:
	# 	f.write(unicode(ET.tostring(root).encode('utf-8')))

	xmlstr = minidom.parseString(unicode(ET.tostring(root).encode('utf-8'))).toprettyxml(indent="   ")
	with  codecs.open("output.xml", "w",encoding="utf-8") as f:
		f.write(unicode(xmlstr.encode('utf-8')))



def load_xml():
	sys.stdout.write('\n')
	sys.stdout.write('\r')
	sys.stdout.write("Loading XML file")
	sys.stdout.flush()
	
	filename = "output.xml"
	with codecs.open(filename, "r",encoding="utf-8") as f:
		xmldoc = minidom.parse(f)
		dictionary = {}

		lemma = xmldoc.getElementsByTagName('lemma')
		for s in lemma:
			y =  s.getElementsByTagName('document')
			for t in y:
				if s.attributes['name'].value in dictionary:
					dictionary[s.attributes['name'].value].append([t.attributes['id'].value,t.attributes['weight'].value])
				else:
					dictionary[s.attributes['name'].value]=[[t.attributes['id'].value,t.attributes['weight'].value]]
		return dictionary
	

def turn_to_power(list, power=1): 
    return [number**power for number in list]

def cosine_similarity(vector1,vector2):
	temp = []
	for i in range(len(vector1)):
		temp.append(vector1[i]*vector2[i])
	nom = sum(temp) 
	den = math.sqrt(sum(turn_to_power(vector1,2))*sum(turn_to_power(vector2,2)))
	return float(nom/den)


def tanimoto(vector1,vector2):
	# (x * y )/ (x*x + y*y - x*y)
	temp = []
	for i in range(len(vector1)):
		temp.append(vector1[i]*vector2[i]) 
	nom = sum(temp)
	den = sum(turn_to_power(vector1,2)) + sum(turn_to_power(vector2,2)) - nom
	return float(nom/den)

def jaccard(vector1,vector2):
	# intersection of lists / union of lists
	u = list(set(vector1).union(vector2))
	i = list(set(vector1).intersection(vector2))
	return float(len(i)/float(len(u)))



def save_weights(weight):
	sys.stdout.write('\n')
	sys.stdout.write('\r')
	sys.stdout.write("Storing in database")
	sys.stdout.flush()
	counter = 0
	keylist = weight.keys()
	keylist.sort()
	dict_length = len(weight)
	for lemma in keylist:
		counter = counter + 1
		sys.stdout.write('\r')
		sys.stdout.write("Processing Lemma %d of %d"%(counter,dict_length))
		sys.stdout.flush()
		cur.execute('INSERT IGNORE INTO InvertedIndex(Word) VALUES ("%s")'%(lemma.encode('utf-8')))
		db.commit()
		cur.execute('SELECT invID FROM InvertedIndex WHERE Word = "%s"'%(lemma.encode('utf-8')))
		id = cur.fetchone()
		for i in weight[lemma]:
			cur.execute("INSERT IGNORE INTO InvertedDocs(ID,doc_id,weight) VALUES (%s,%s,%s)",(str(id[0]).encode('utf-8'),str(i[0]).encode('utf-8'),str(i[1]).encode('utf-8')))
			db.commit()

def create_vectors(weight):
	# find each word's max tfxidf and then order all words so as to get top-8000

	cur.execute("SELECT word FROM InvertedIndex as c INNER JOIN ((SELECT ID as b ,MAX(weight) as weight FROM InvertedDocs GROUP BY ID ORDER BY weight DESC) as tmp) ON invID= b ORDER BY `tmp`.`weight` DESC,word ASC LIMIT 8000 ") #OFFSET
	#SELECT word,weight FROM InvertedIndex as c INNER JOIN ((SELECT ID as b ,MAX(weight) as weight FROM InvertedDocs GROUP BY ID ORDER BY weight DESC) as tmp) ON invID= b ORDER BY `tmp`.`weight` DESC,word ASC LIMIT 8000
	i = 0
	for word in cur.fetchall():
		x.append(word[0])
		i = i +1 
	j = 0
	
	# get ids of all mails
	cur.execute('SELECT URL FROM Mails')
	for mail in cur.fetchall():
		a_list = []
		b_list = []
		# get all stored lemmas
		y.append(mail[0])
		for j in range(len(x)):
			flag = 0
			# iterate through reverse index to check if a mail's id exists and store tfxidf
			for i in weight[x[j].encode('utf-8')]:
				if str(i[0]) == str(mail[0]):
					flag = 1
					a_list.append (float(i[1]))
					b_list.append(x[j])
			if flag == 0:
				a_list.append(0)
		# normalization of a_list = sqrt(sum(a_list^2))
		tsum = 0
		for i in range(len(a_list)):
			tsum = tsum + math.pow(float(a_list[i]),2)
		den = math.sqrt(tsum)
		for i in range(len(a_list)):
			a_list[i] = float(a_list[i])/float(den)
		list_of_lists.append(list(a_list))
		list_of_words.append(list(b_list))



def calculate_similarity():
	N = len(list_of_lists)
	# code to open test_files
	success = 0
	total = 0

	path = "/home/andronikos/Downloads/20news-bydate/20news-bydate-test/selection"
	#path = "/home/andronikos/Downloads/20news-bydate/test2"
	for folder in os.listdir(path):
		t_total = 0
		t_success = 0
		print "\nTesting from: " + folder
		for file in os.listdir(path+"/"+folder):
			name = path +"/"+folder+"/"+file
			with codecs.open(name, 'r',encoding='utf-8', errors='ignore') as f:
				mail = f.read()
				#Insert Articles & Words in Database
				lmtzr = WordNetLemmatizer()
	 			url = file
				text = mail.decode('utf-8')
				text = string.replace(text,'/',' ')
				text = string.replace(text,'\\',' ')
				text = string.replace(text,'*',' ')
				text = string.replace(text,'@',' ')
				text = string.replace(text,'.',' ')
				text = string.replace(text,'=',' ')
				text = string.replace(text,'^',' ')
				text = string.replace(text,'%',' ')
				text = string.replace(text,',',' ')
				text = string.replace(text,'+',' ')
				text = string.replace(text,'_',' ')
				text = string.replace(text,"'",' ')
				text = string.replace(text,'-',' ')
				text = string.replace(text,'!',' ')
				text = text.lower()
				i=0;
				vector = []
				lemmas = []
				tokens = nltk.word_tokenize(text) #tokenize 
				tagged = nltk.pos_tag(tokens) 	   #tag each token
				#find postag
				for word in tokens:
					verb_tags = {"VB","VBD","VBG","VBN","VBP","VBZ"}
					if tagged[i][1] in verb_tags:
						lword = lmtzr.lemmatize(word,pos='v')
					else:
						lword = lmtzr.lemmatize(word)
					#choose if tag belongs in open or close list
					open_tags = {"JJ","JJR","JJS","RB","RBR","RBS","NN","NNS","NNP","NNPS","VB","VBD","VBG","VBN","VBP","VBZ","FW"}
					if tagged[i][1] in open_tags:
						lemmas.append(lword)
					i = i + 1

				#calculate term frequencies and also aggreagate lemmas
				counts = Counter(lemmas)
				word_list = list(counts.keys())
				max_f = max(counts.values())
				#iterate x in order to find top lemmas
				for i in range(len(x)):
					# if a lemma is present - calculate tf X idf
					if x[i] in lemmas:
						
						tf = float(float(counts[x[i]])/float(max_f))
						# tf = float(counts[x[i]])
						cur.execute('SELECT COUNT(DISTINCT ID) FROM Words WHERE Lemma = "%s"'%(x[i].encode('utf-8')))
						df = cur.fetchone()
						vector.append(float(tf) * float(log10(float(N)/float(df[0]))))
					else:
						vector.append(0)
				# normalization of vector = sqrt(sum(vector^2))
				tsum = 0
				for i in range(len(vector)):
					tsum = tsum + math.pow(float(vector[i]),2)
				den = math.sqrt(tsum)
				for i in range(len(vector)):
					vector[i] = float(vector[i])/float(den)


				# compare to all vectors and find the one with the maximum similarity
				max_comp= 0
				ind = 0
				for i in range(len(y)):
					#sim = cosine_similarity(vector,list_of_lists[i])
					sim = tanimoto(vector,list_of_lists[i])

					#jaccard must take as input list of words
					#sim = jaccard(word_list,list_of_words[i])

					if ( sim > max_comp ):
						max_comp = sim
						ind = i

				#print ind
				max_id = y[ind]
				#print max_id , max_comp
				cur.execute('SELECT Category FROM Mails WHERE URL = "%s"'%(str(max_id).encode('utf-8')))
				category = cur.fetchone()

				total = total + 1 
				t_total = t_total + 1
				if category[0] == folder:
					success = success + 1
					t_success = t_success +1

				sys.stdout.write('\r')
				sys.stdout.write("Successfull %d of %d - %f %%  -  "%(t_success,t_total,100*float(t_success)/float(t_total)))
				sys.stdout.flush()
				sys.stdout.write("Overall Successfull %d of %d - %f %%"%(success,total,100*float(success)/float(total)))
				sys.stdout.flush()



				# TEST DATA FOR 0.99 similarity 
				# print x
				# print vector
				# vector2 = list_of_lists[y.index(37261)]
				# print "\n", vector2
				# print vector == vector2
				# print sum(map(float,vector))
				# print sum(map(float,vector2))
				# print cosine_similarity(vector,vector2)


#load_database()
#weight = calculate_tf_idf()
#export_to_xml(weight)
dictionary = load_xml()
#save_weights(dictionary)

# dictionary = load_xml()
create_vectors(dictionary)
calculate_similarity()



cur.close()
db.close()