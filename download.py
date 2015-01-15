import sqlite3 as sql
from urllib2 import Request, urlopen, URLError
from bs4 import BeautifulSoup
import os, re, warnings, codecs, sys, shutil

THREAD_URL = "https://bitcointalk.org/index.php?topic={}.0;all"
BOARD_URL = "https://bitcointalk.org/index.php?board={}.{}"

#converts HTML entities to unicode.
def H2U(text):
	with warnings.catch_warnings():
		warnings.simplefilter("ignore")
		return BeautifulSoup(text).get_text()

#returns html content, either from a cache or from the web
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
		print("unable to open " + url)
		req = Request(url)
		return (False, "")
	else:
		return (True, response.read())

def fetchHTML(url):
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
	command = '''CREATE TABLE IF NOT EXISTS ForeignBoards (
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
	CREATE TABLE IF NOT EXISTS ForeignThreads (thread_id Integer primary key)
	'''
	cur.execute(command)
	command = '''
	CREATE TABLE IF NOT EXISTS ThreadIds (thread_id Integer primary key)
	'''
	cur.execute(command)

def addBoards(cur):
	f = open('boards.txt', 'r')
	foreign = False
	for rline in f.readlines():
		line = rline.strip()
		regex = r'board=(.*)"'
		m = re.search(regex, line)
		bid = int(float(m.group(1)))
		regex = r'=> (.*)<'
		m = re.search(regex, line)
		bname = H2U(m.group(1))
		if not foreign:
			command = '''INSERT OR IGNORE INTO Boards (board_id, name)
			VALUES (?,?)
			'''
		else:
			command = '''INSERT OR IGNORE INTO ForeignBoards (board_id, name)
			VALUES (?,?)'''
		cur.execute(command, (bid, bname))
		if bid == 198:
			foreign = True

def isValidThread(content):
	return ('The topic or board you are looking for appears to be either missing or off limits to you.' not in content)

#parses html for data. If parent board id is in ignoreBoards, return False
def parseThread(content, ignoreBoards):
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
				if parent_bid in ignoreBoards:
					return False
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

def downloadThread(thread_id, ignoreBoards, cur):
	htmlresult = fetchThreadHTML(thread_id)
	if not htmlresult[0]:
		return False
	content = htmlresult[1]
	if not isValidThread(content):
		command = '''INSERT OR IGNORE INTO InvalidThreads (thread_id) VALUES (?)'''
		cur.execute(command, (thread_id,))
		return False
	vals = parseThread(content, ignoreBoards)
	if vals == False:
		command = '''INSERT OR IGNORE INTO ForeignThreads (thread_id) VALUES (?)'''
		cur.execute(command, (thread_id,))
		return False
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

def removeForeignThreads(cur):
	command = '''SELECT board_id FROM ForeignBoards'''
	cur.execute(command)
	fbs = cur.fetchall()
	command = '''SELECT thread_id FROM Threads WHERE parent_bid=?'''
	command2 = '''INSERT INTO ForeignThreads (thread_id) VALUES (?)'''
	command3 = '''DELETE FROM Threads WHERE parent_bid=?'''
	command4 = '''DELETE FROM Boards WHERE board_id=?'''
	for fb in fbs:
		cur.execute(command, fb)
		ts = cur.fetchall()
		cur.executemany(command2, ts)
	cur.executemany(command3, fbs)
	cur.executemany(command4, fbs)

def countThreads(cur):
	command = '''SELECT COUNT(*) FROM Threads'''
	cur.execute(command)
	return cur.fetchone()[0]

def getIgnoreThreads(cur):
	command = '''SELECT thread_id FROM Threads
	UNION SELECT thread_id FROM InvalidThreads
	UNION SELECT thread_id FROM ForeignThreads'''
	cur.execute(command)
	return set([i[0] for i in cur.fetchall()])

def getIgnoreBoards(cur):
	command = '''SELECT board_id from ForeignBoards'''
	cur.execute(command)
	return set([i[0] for i in cur.fetchall()])

def getPageTopics(boardId, page):
	topics = set()
	url = BOARD_URL.format(boardId, str(page*40))
	html = fetchHTML(url)[1]
	regex = r'\?topic=([1-9]*)\.'
	for line in html.split('\n'):
		m = re.search(regex, line)
		if m is not None:
			topics.add(int(m.group(1)))
	return topics

def getBoardPages(html):
	for line in html.split('\n'):
		if 'id="toppages"' in line:
			break
	regex = r'>([1-9]*)<'
	return range(int(re.findall(regex, line)[-1]))[1:]

#returns a list of ids of all the topics in a board
def getAllTopics(boardId, con):
	topics = []
	numThreads = countThreads(con.cursor())
	html = fetchHTML(BOARD_URL.format(boardId, '0'))
	if not html[0]:
		return topics
	else:
		html = html[1]
	pages = getBoardPages(html)
	ignoreThreads = getIgnoreThreads(con.cursor())
	command = "INSERT OR IGNORE INTO ThreadIds (thread_id) VALUES (?)"
	for page in pages[150:]:
		print(page)
		topics = getPageTopics(boardId, page)
		for topic in topics:
			with con:
				con.cursor().execute(command, (topic,))

def downloadAllTopics(con):
	command = '''SELECT thread_id FROM ThreadIds
	WHERE thread_id NOT IN (
	SELECT thread_id FROM Threads)'''
	cur = con.cursor()
	cur.execute(command)
	todl = [i[0] for i in cur.fetchall()][::-1]
	for t in todl:
		with con:
			sys.stdout.write("\rdownloading thread {}".format(t))
			sys.stdout.flush()
			downloadThread(t, [], cur)

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
		ignoreThreads = getIgnoreThreads(cur)
		ignoreBoards = getIgnoreBoards(cur)
	with con:
		getAllTopics(1, con)
		downloadAllTopics(con)

if __name__=='__main__':
	main()
