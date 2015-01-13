#from __future__ import print_function
import sqlite3 as sql
from urllib2 import Request, urlopen, URLError
from bs4 import BeautifulSoup
import os, re, warnings, codecs, sys, shutil

THREAD_URL = "https://bitcointalk.org/index.php?topic={}.0;all"

#converts HTML entities to unicode.
def H2U(text):
	with warnings.catch_warnings():
		warnings.simplefilter("ignore")
		return BeautifulSoup(text).get_text()

def fetchHTML(tid):
	tid = str(tid)
	path = os.path.join('htmldata', tid+'.html')
	if os.path.exists(path):
		f = open(path, 'r')
		return (True, f.read())
	url = THREAD_URL.format(tid)
	req = Request(url)
	try:
		response = urlopen(req)
	except URLError as e:
		print("unable to open " + url)
		req = Request(url)
		return (False, "")
	else:
		return (True, response.read())

def createTables(cur):
	command = '''CREATE TABLE IF NOT EXISTS Boards (
	board_id INTEGER PRIMARY KEY,
	name TEXT)'''
	cur.execute(command)
	command = '''
	CREATE TABLE IF NOT EXISTS Threads (
	thread_id Integer primary key,
	topic Text,
	html_path Text,
	posts Text,
	parent_bid Integer,
	op_date Text,
	html_content Blob,
	foreign key (parent_bid) references Boards(board_id))
	'''
	cur.execute(command)
	command = '''
	CREATE TABLE IF NOT EXISTS InvalidThreads (thread_id Integer primary key)
	'''
	cur.execute(command)

def addBoards(cur):
	f = open('boards.txt', 'r')
	for rline in f.readlines():
		line = rline.strip()
		regex = r'board=(.*)"'
		m = re.search(regex, line)
		bid = int(float(m.group(1)))
		regex = r'=> (.*)<'
		m = re.search(regex, line)
		bname = H2U(m.group(1))
		command = '''INSERT OR IGNORE INTO Boards (board_id, name)
		VALUES (?,?)
		'''
		cur.execute(command, (bid, bname))

def isValidThread(content):
	return ('The topic or board you are looking for appears to be either missing or off limits to you.' not in content)

def parseThread(content):
	parent_bid = -1
	title = u''
	date = u''
	posts = []
	for line in content.split('\n'):
		line = line.replace('\t', '')
		#find the parent board id
		if parent_bid == -1:
			regex = r'option value\="\?board\=(.*)" selected\="selected"'
			m = re.search(regex, line)
			if m is not None:
				parent_bid = int(float(m.group(1)))
				"""
				if parent_bid not in [1,7,57,34,14]:
					return False
				"""
		#find the thread title
		if title == "":
			regex = r'<title>(.*)</title>'
			m = re.search(regex, line)
			if m is not None:
				title = H2U(m.group(1))
		if date == "":
			regex = r'<div class="smalltext">(.*20[0-2][0-9].*)</div>'
			m = re.search(regex, line)
			if m is not None:
				date = H2U(m.group(1))
		regex = r'<div class="post">(.*)</div>'
		m = re.search(regex, line)
		if m is not None:
			posts.append(H2U(m.group(1)))
	postsString = '\n'.join(posts)
	return (title, postsString, parent_bid, date)

def downloadThread(thread_id, cur):
	htmlresult = fetchHTML(thread_id)
	if not htmlresult[0]:
		return False
	content = htmlresult[1]
	if not isValidThread(content):
		command = '''INSERT INTO InvalidThreads (thread_id) VALUES (?)'''
		cur.execute(command, (thread_id,))
		return False
	vals = parseThread(content)
	if vals == False:
		return False
	topic, posts, parent_bid, op_date = vals
	path = os.path.join("data","temp.temp")
	with open(path, 'w') as f:
		f.write(content)
	with open(path, 'rb') as f:
		ablob = f.read()
	command = '''INSERT INTO Threads 
	(thread_id, topic, posts, parent_bid, op_date, html_content)
	VALUES(?,?,?,?,?,?)'''
	cur.execute(command, \
			(thread_id, topic, posts, parent_bid, op_date, sql.Binary(ablob)))
	return True

def countThreads(cur):
	command = '''SELECT COUNT(*) FROM Threads'''
	cur.execute(command)
	return cur.fetchone()[0]

def main():
	if len(sys.argv) != 2:
		print("Usage: db_name")
		return
	path = sys.argv[1]
	dbExists = os.path.exists(path)
	con = sql.connect(path)
	with con:
		cur = con.cursor()
		createTables(cur)
		addBoards(cur)
		command = '''SELECT thread_id FROM Threads'''
		cur.execute(command)
		ignore = set([i[0] for i in cur.fetchall()])
		command = '''SELECT thread_id FROM InvalidThreads'''
		cur.execute(command)
		ignore.update([i[0] for i in cur.fetchall()])
	numThreads = countThreads(con.cursor())
	for i in range(903800)[::-1]:
		if i not in ignore:
			with con:
				cur = con.cursor()
				if downloadThread(i, cur):
					numThreads += 1
					sys.stdout.write("\rStored thread \t\t{}\tTotal threads: {}            ".format(i, numThreads))
					sys.stdout.flush()
				else:
					sys.stdout.write("\rDidn't store thread \t{}\tTotal threads: {}            ".format(i, numThreads))
					sys.stdout.flush()

if __name__=='__main__':
	main()
