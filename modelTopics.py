import sqlite3 as sql
import os, sys, codecs, logging, itertools, pickle
from collections import defaultdict
from operator import itemgetter
from gensim import corpora, models
from gensim.utils import smart_open, simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)
logging.root.level = logging.INFO

STOPWORDS2 = ['january','february','march','april','may','june','july','august','september','october','november','december'] + list(STOPWORDS)
NUM_TOPICS = 50


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

def modelTopics(cpath, dpath, mpath):
	print("Modeling topics...")
	mm_corpus = corpora.MmCorpus(cpath)
	id2word_docs = corpora.Dictionary.load(dpath)
	lda_model = models.LdaModel(mm_corpus, num_topics=NUM_TOPICS, id2word=id2word_docs, passes=100, iterations=50)
	lda_model.save(mpath)
	print(lda_model.print_topics(-1))

def returnlist():
	return []

def exportTopics(dbpath, dpath, mpath):
	topicmap = {}
	lda_model = models.LdaModel.load(mpath)
	dictionary = corpora.Dictionary.load(dpath)
	for thread in iter_tokdocs(dbpath):
		topicmap[thread[0]] = lda_model[dictionary.doc2bow(thread[1])]
		sys.stdout.write('\r' + str(thread[0]))
		sys.stdout.flush()
	with open('topicmap.pickle', 'wb') as f:
		pickle.dump(topicmap, f)
	with open('topicmap.pickle', 'rb') as f:
		topicmap = pickle.load(f)
	topicid2topic = {}
	for i in range(NUM_TOPICS):
		topicid2topic[i] = '_'.join([w[1] for w in lda_model.show_topic(i)])
	topic2threads = defaultdict(returnlist)
	for thread in topicmap.keys():
		topics = topicmap[thread]
		for topic in topics:
			topic2threads[topic[0]].append((thread, topic[1]))
	for top in topic2threads.keys():
		if type(topic2threads[top]) is not list:
			print("ERROR")
			return
	con = sql.connect(dbpath)
	if not os.path.exists("topics"):
		os.mkdir("topics")
	for topid in topic2threads.keys():
		topicstring = topicid2topic[topid]
		topicpath = os.path.join("topics", topicstring)
		if not os.path.exists(topicpath):
			os.mkdir(topicpath)
		matches = sorted(topic2threads[topid], key=itemgetter(1), reverse=True)
		for i in range(5):
			tid = matches[i][0]
			exportThread(con, tid, os.path.join(topicpath,str(tid))+'.html')

def main():
	if len(sys.argv) != 2:
		print("Usage: db_name")
		return
	dbpath = sys.argv[1]
	if not os.path.exists(dbpath):
		print("Error: database does not exist")
	if not os.path.exists("data"):
		os.mkdir("data")
	cpath = os.path.join("data", "threads_bow.mm")
	dpath = os.path.join("data", "threads_dict.dict")
	mpath = os.path.join("data", "threads_model.lda")
	exportCorpus(dbpath, cpath, dpath)
	modelTopics(cpath, dpath, mpath)
	exportTopics(dbpath, dpath, mpath)

if __name__=='__main__':
	main()
