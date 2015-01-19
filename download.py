import sqlite3 as sql
from urllib2 import Request, urlopen, URLError
from bs4 import BeautifulSoup
import os, re, warnings, codecs, sys, shutil
from sys import stdout
from time import sleep

THREAD_URL = "https://bitcointalk.org/index.php?topic={}.0;all"
BOARD_URL = "https://bitcointalk.org/index.php?board={}.{}"

#converts HTML entities to unicode.
def H2U(text):
	with warnings.catch_warnings():
		warnings.simplefilter("ignore")
		return BeautifulSoup(text).get_text()

#returns html content of a thread, either from a cache or from the web
def fetchThreadHTML(tid):
	tid = str(tid)
	path = os.path.join('htmldata', tid+'.html')
	if os.path.exists(path):
		with open(path, 'r') as f:
			contents = f.read()
		os.remove(path)
		return (True, contents)
	url = THREAD_URL.format(tid)
	req = Request(url)
	try:
		response = urlopen(req)
	except URLError as e:
		req = Request(url)
		return (False, "")
	else:
		return (True, response.read())

def fetchHTML(url):
	req = Request(url)
	try:
		response = urlopen(req)
	except URLError as e:
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
	command = '''
	CREATE TABLE IF NOT EXISTS ThreadIds (thread_id Integer primary key)
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
		VALUES (?,?)'''
		cur.execute(command, (bid, bname))

def isValidThread(content):
	return ('The topic or board you are looking for appears to be either missing or off limits to you.' not in content)

#parses html for data
def parseThread(content):
	parent_bid = -1
	title = u''
	date = u''
	posts = []
	for line in content.split('\n'):
		line = line.replace('\t', '')
		if parent_bid == -1:
			regex = r'option value\="\?board\=(.*)" selected\="selected"'
			m = re.search(regex, line)
			if m is not None:
				parent_bid = int(float(m.group(1)))
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

#downloads a thread and adds its data to the database
def downloadThread(thread_id, cur):
	htmlresult = fetchThreadHTML(thread_id)
	if not htmlresult[0]:
		return False
	content = htmlresult[1]
	if not isValidThread(content):
		command = '''INSERT OR IGNORE INTO InvalidThreads (thread_id) 
		VALUES (?)'''
		cur.execute(command, (thread_id,))
		return False
	vals = parseThread(content)
	topic, posts, parent_bid, op_date = vals
	path = os.path.join("data","temp.temp")
	with open(path, 'w') as f:
		f.write(content)
	with open(path, 'rb') as f:
		ablob = f.read()
	command = '''INSERT OR IGNORE INTO Threads 
	(thread_id, topic, posts, parent_bid, op_date, html_content)
	VALUES(?,?,?,?,?,?)'''
	cur.execute(command, \
			(thread_id, topic, posts, parent_bid, op_date, sql.Binary(ablob)))
	return True

#returns the number of threads in the database
def countThreads(cur):
	command = '''SELECT COUNT(*) FROM Threads'''
	cur.execute(command)
	return cur.fetchone()[0]

#get the threads we don't need to download
def getIgnoreThreads(cur):
	command = '''SELECT thread_id FROM Threads
	UNION SELECT thread_id FROM InvalidThreads'''
	cur.execute(command)
	return set([i[0] for i in cur.fetchall()])

#returns the integer ids of all the topics on a page of a board
def getPageTopics(boardId, page):
	topics = set()
	url = BOARD_URL.format(boardId, str(page*40))
	html = fetchHTML(url)[1]
	#html = fetchPageHTML(page)[1]
	regex = r'\?topic=([0-9]*)\.'
	for line in html.split('\n'):
		topics.update([int(m) for m in re.findall(regex, line)])
	return topics

#returns the integer page numbers of all the pages in the board
def getBoardPages(html):
	for line in html.split('\n'):
		if 'id="toppages"' in line:
			break
	regex = r'>([1-9]*)<'
	return range(int(re.findall(regex, line)[-1]))[::-1]

#gets the ids of all the topics in a board, and adds them to the database
def getAllTopics(boardId, con):
	topics = []
	numThreads = countThreads(con.cursor())
	html = fetchHTML(BOARD_URL.format(boardId, '0'))
	html = html[1]
	pages = getBoardPages(html)
	#pages = range(563)[::-1]
	command = "INSERT OR IGNORE INTO ThreadIds (thread_id) VALUES (?)"
	l = len(pages)
	for i, p in enumerate(pages):
		stdout.write(
			"\rdiscovering threads: {}% done (page {})".format(int(100*i/l),p))
		stdout.flush()
		topics = getPageTopics(boardId, p)
		for topic in topics:
			with con:
				con.cursor().execute(command, (topic,))

def downloadAllTopics(con):
	command = '''SELECT thread_id FROM ThreadIds
	WHERE thread_id NOT IN (
	SELECT thread_id FROM Threads)'''
	cur = con.cursor()
	cur.execute(command)
	todl = [i[0] for i in cur.fetchall()]#[::-1]
	l = len(todl)
	for i, t in enumerate(todl):
		stdout.write(
		 "\rdownloading threads: {}% done (thread {})".format(int(100*i/l),t))
		stdout.flush()
		with con:
			downloadThread(t, cur)

def addFromFolder(con):
	files = os.listdir("htmldata")
	for f in files:
		i = f.split('.')[0]
		with con:
			downloadThread(i, con.cursor())
		stdout.write("\rdownloading thread {}".format(i))
		stdout.flush

def main():
	if len(sys.argv) != 3:
		print("Usage: python download.py <db_name> <board_id>")
		return
	path = sys.argv[1]
	boardId = sys.argv[2]
	dbExists = os.path.exists(path)
	con = sql.connect(path)
	with con:
		cur = con.cursor()
		createTables(cur)
		addBoards(cur)
	getAllTopics(boardId, con)
	#addFromFolder(con)
	downloadAllTopics(con)
	print("Done downloading topics!")

if __name__=='__main__':
	main()
