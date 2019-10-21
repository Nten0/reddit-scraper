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
import string
from warnings import filterwarnings
filterwarnings('ignore', category = MySQLdb.Warning)

db = MySQLdb.connect(host = "localhost",
					user = "root",
					passwd = "root",
					db = "Project_LP",charset='utf8')

cur = db.cursor()



#Code to open /artcles/*
#with open('articles.json', 'r') as f:
def load_database():
	
	num_of_articles_processed = 0
	num_of_articles_inserted = 0

	path = "/home/andronikos/articles"
	for file in os.listdir(path):
		name = path +"/"+file
		with open(name, 'r') as f:
			articles = json.load(f)
			#Insert Articles & Words in Database
			lmtzr = WordNetLemmatizer()
			for article in articles:
				if article['text'].strip(): #if text is not empty
					encoded_url = base64.urlsafe_b64encode(article['url'][0]) #encode url in order to achieve uniquity
					url = article['url'][0]
					title = article['title'][0]
					text = article['text']
					text = string.replace(text,'/',' ')
					text = string.replace(text,'\\',' ')
					text = string.replace(text,'*',' ')
					text = string.replace(text,'@',' ')
					text = string.replace(text,'.',' ')
					text = string.replace(text,'=',' ')
					text = string.replace(text,'^',' ')
					text = string.replace(text,'%',' ')
					text = text.lower()
					i=0;
					
					#Insert Article into Database
					cur.execute("INSERT IGNORE INTO Articles(URL,Title,MainText,Hash) VALUES (%s,%s,%s,%s)",(url.encode('utf-8'),title.encode('utf-8'),text.encode('utf-8'),encoded_url.encode('utf-8')))
					db.commit()
					num_of_articles_processed = num_of_articles_processed +1
					last_id = cur.lastrowid
					#If insertion is successful
					if last_id != 0:
						num_of_articles_inserted = num_of_articles_inserted + 1
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
								category = 0
							else:
								category = 1
							# Insert Words of each Article into Database
							cur.execute("INSERT IGNORE INTO Words(ID,Word,Tag,Lemma,OpenClose) VALUES (%s,%s,%s,%s,%s)",(last_id,word.encode('utf-8'),tagged[i][1].encode('utf-8'),lword.encode('utf-8'),category))
							db.commit()
							i = i + 1
					sys.stdout.write('\r')
					sys.stdout.write("Inserted Articles/Processed Articles: %d / %d" %(num_of_articles_inserted,num_of_articles_processed))
					sys.stdout.flush()

#Calculate Max Frequency of term in each Article
def calculate_tf():
	cur.execute("SELECT COUNT(ID) FROM Articles ")
	num_of_articles_inserted = cur.fetchone()[0]
	proc_art = 0
	tmp1 = {}
	weight = defaultdict(list)
	cur.execute("SELECT ID FROM Articles ORDER BY ID")
	sys.stdout.write('\n')
	for id in cur.fetchall():
		proc_art = proc_art + 1
		sys.stdout.write('\r')
		sys.stdout.write("Processing Article %d of %d - %f%%" %(proc_art,num_of_articles_inserted,100*float(proc_art/float(num_of_articles_inserted))))
		sys.stdout.flush()
		#Calculate Max frequency of term in an article
		cur.execute("SELECT MAX(counted) FROM ( SELECT COUNT(*)AS counted FROM Words WHERE ID = '%d' AND OpenClose = 0 GROUP BY Lemma ) AS counts"%(id))
		maxf = cur.fetchone()[0]
		#print "Max Frequency: ",maxf
		# find tf of each term = term frequency / max frequency and insert to dictionary
		tmp2 = {}
		cur.execute("SELECT Lemma,COUNT(*) FROM Words WHERE ID = '%d' AND OpenClose = 0 GROUP BY Lemma"%(id))
		tmp2 = {lemma:[id[0],count/float(maxf)] for (lemma,count) in cur.fetchall()}
		# merge dictionaries together
		for d in (tmp1,tmp2): 
			for key, value in d.iteritems():
				weight[key].append(value)

	return weight




def calculate_tf_idf(weight):
	#print weight
	# find number of articles stored in databse (N)
	cur.execute("SELECT COUNT(*) FROM Articles ORDER BY ID")
	N = cur.fetchone()
	#print "Number of Articles: ",N[0]

	# find number of times a term is stored in database and calculate tf x idf
	cur.execute("SELECT Lemma,COUNT(DISTINCT ID) FROM Words WHERE OpenClose = 0 GROUP BY Lemma")
	#SELECT Lemma,COUNT(*) FROM Words WHERE OpenClose = 0 GROUP BY Lemma
	for lemma,n in cur.fetchall():
		for i in weight[lemma]:
			#calculate tf x idf
			i[1] = float(i[1]) * float(log10(float(N[0])/float(n))) #check log base
		#print lemma ,weight[lemma]
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


	xmlstr = minidom.parseString(ET.tostring(root)).toprettyxml(indent="   ")
	with open("output.xml", "w") as f:
	    f.write(xmlstr.encode('utf-8'))


def store_in_db(weight):

	keylist = weight.keys()
	keylist.sort()

	sys.stdout.write('\n')
	sys.stdout.write('\r')
	sys.stdout.write("Storing in database ...")
	sys.stdout.flush()

	sys.stdout.write('\n')
	counter = 0
	dict_length = len(weight)
	for lemma in keylist:
		counter = counter + 1
		sys.stdout.write('\r')
		sys.stdout.write("Processing Lemma %d of %d"%(counter,dict_length))
		sys.stdout.flush()
		#cur.execute("INSERT IGNORE INTO Articles(URL,Title,MainText,Hash) VALUES (%s,%s,%s,%s)",(url.encode('utf-8'),title.encode('utf-8'),text.encode('utf-8'),encoded_url.encode('utf-8')))
		cur.execute('INSERT IGNORE INTO InvertedIndex(Word) VALUES ("%s")'%(lemma.encode('utf-8')))
		db.commit()
		cur.execute('SELECT invID FROM InvertedIndex WHERE Word = "%s"'%(lemma.encode('utf-8')))
		id = cur.fetchone()
		for i in weight[lemma]:
			cur.execute("INSERT IGNORE INTO InvertedDocs(ID,doc_id,weight) VALUES (%s,%s,%s)",(str(id[0]).encode('utf-8'),str(i[0]).encode('utf-8'),str(i[1]).encode('utf-8')))
			db.commit()
	cur.close()
	db.close()


load_database()
weight = calculate_tf()
weight = calculate_tf_idf(weight)
export_to_xml(weight)
store_in_db(weight)
