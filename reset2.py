import MySQLdb
db = MySQLdb.connect(host = "localhost",
					user = "root",
					passwd = "root",
					db = "Project_LP2",charset='utf8')

cur = db.cursor()
cur.execute("DELETE FROM Mails")
db.commit()
cur.execute("DELETE FROM InvertedDocs ")
db.commit()
cur.execute("DELETE FROM InvertedIndex")
db.commit()
cur.execute("DELETE FROM Words")
db.commit()
cur.execute("ALTER TABLE Mails AUTO_INCREMENT = 1")
db.commit()
cur.execute("ALTER TABLE Words AUTO_INCREMENT = 1")
db.commit()
cur.execute("ALTER TABLE InvertedIndex AUTO_INCREMENT = 1")
db.commit()
cur.close()
db.close()