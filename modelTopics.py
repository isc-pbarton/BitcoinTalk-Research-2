import sqlite3 as sql
import os, sys, codecs, logging, itertools, pickle
from sys import stdout
from collections import defaultdict
from operator import itemgetter
from gensim import corpora, models
from gensim.utils import smart_open, simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)
logging.root.level = logging.INFO

STOPWORDS2 = set(['january','february','march','april','may','june','july','august','september','october','november','december'])
STOPWORDS2.update(list(STOPWORDS))

def tokenize(text):
	return [token for token in simple_preprocess(text) \
					if token not in STOPWORDS2]

#iterates over all threads, returning a (tid, tokens) tuple
def iter_tokdocs(path):
	con = sql.connect(path)
	cur = con.cursor()
	command = '''SELECT thread_id FROM Threads WHERE parent_bid=1'''
	cur.execute(command)
	threads = cur.fetchall()
	for tid in threads:
		command = '''SELECT posts FROM Threads t 
		WHERE t.thread_id = ?'''
		cur.execute(command, (tid[0],))
		text = cur.fetchone()
		tokens = tokenize(text[0])
		if len(tokens) > 200:
			yield tid[0], tokens

def len_tokdocs(path):
	con = sql.connect(path)
	cur = con.cursor()
	command = '''SELECT COUNT(*) FROM Threads WHERE parent_bid=1'''
	cur.execute(command)
	return cur.fetchone()[0]

class ThreadsCorpus(object):
	def __init__(self, db_path, dictionary, clip_docs=None):
		self.db_path = db_path
		self.dictionary = dictionary
		self.clip_docs = clip_docs

	def __iter__(self):
		self.titles = []
		for title, tokens in itertools.islice(iter_tokdocs(self.db_path), self.clip_docs):
			self.titles.append(title)
			yield self.dictionary.doc2bow(tokens)
	def __len__(self):
		return len_tokdocs(db_path)

def exportThread(con, tid, path):
	command = '''
	SELECT html_content FROM Threads WHERE thread_id=?
	'''
	with con:
		cur = con.cursor()
		cur.execute(command, (tid,))
		ablob = cur.fetchone()
	with open(path, 'wb') as f:
		f.write(ablob[0])

def exportCorpus(dbpath, cpath, dpath):
	print("Creating corpus...")
	id2word = corpora.Dictionary(tokens for _, tokens in iter_tokdocs(dbpath))
	id2word.filter_extremes(no_below=20, no_above=0.2)
	id2word.save(dpath)
	threads_corpus = ThreadsCorpus(dbpath, id2word)
	corpora.MmCorpus.serialize(cpath, threads_corpus)

def modelTopics(cpath, dpath, mpath, numTopics):
	print("Modeling topics...")
	mm_corpus = corpora.MmCorpus(cpath)
	id2word_docs = corpora.Dictionary.load(dpath)
	lda_model = models.LdaModel(mm_corpus, num_topics=numTopics, id2word=id2word_docs, passes=100, iterations=50)
	lda_model.save(mpath)
	print(lda_model.print_topics(-1))

def returnlist():
	return []

def exportTopics(dbpath, dpath, mpath):
	th2to = {}
	to2th = {}
	lda_model = models.LdaModel.load(mpath)
	dictionary = corpora.Dictionary.load(dpath)
	topics = ['_'.join([w[1] for w in lda_model.show_topic(i)]) for i in range(lda_model.num_topics)]
	l = len_tokdocs(dbpath)
	for i, thread in enumerate(iter_tokdocs(dbpath)):
		th2to[thread[0]] = lda_model[dictionary.doc2bow(thread[1])]
		stdout.write('\rtransforming topics: {}%'.format(int(100*i/l)))
		stdout.flush()
	#"""
	to2th = defaultdict(returnlist)
	for thread in th2to.keys():
		for topic in th2to[thread]:
			to2th[topic[0]].append((thread, topic[1]))
	#"""
	#to2th[topic[0]] = [[(h, o[1]) for o in th2to[h]] for h in th2to.keys()]
	con = sql.connect(dbpath)
	if not os.path.exists("topics"):
		os.mkdir("topics")
	for topid in to2th.keys():
		topicpath = os.path.join("topics", topics[topid])
		if not os.path.exists(topicpath):
			os.mkdir(topicpath)
		m = sorted(to2th[topid], key=itemgetter(1), reverse=True)
		for i in range(5):
			tid = m[i][0]
			filename = str(m[i][0]) + '.html'
			exportThread(con, tid, os.path.join(topicpath, filename))

def main():
	if len(sys.argv) != 3:
		print("Usage: python modelTopics.py <db_name> <num_topics>")
		return
	dbpath = sys.argv[1]
	numTopics = int(sys.argv[2])
	if not os.path.exists(dbpath):
		print("Error: database does not exist")
	if not os.path.exists("data"):
		os.mkdir("data")
	cpath = os.path.join("data", "threads_bow.mm")
	dpath = os.path.join("data", "threads_dict.dict")
	mpath = os.path.join("data", "threads_model.lda")
	#exportCorpus(dbpath, cpath, dpath)
	#modelTopics(cpath, dpath, mpath, numTopics)
	exportTopics(dbpath, dpath, mpath)

if __name__=='__main__':
	main()
