import sqlite3 as sql
import os
from os.path import isfile, join

dbpath = raw_input("input database path: ")
htmls = [f for f in os.listdir('htmlfiles') if isfile(join('htmlfiles',f)) ]
con = sql.connect(dbpath)
command = '''
UPDATE Threads SET html_content=? WHERE thread_id=?
'''
for html in htmls:
	tid = unicode(html.split('.')[0])
	f = open(join('htmlfiles',html), 'rb')
	ablob = f.read()
	with con:
		cur = con.cursor()
		cur.execute(command, (sql.Binary(ablob), tid))
	f.close()
		
