# BitcoinTalk-Research-2
This is a program that performs topic modeling on the web forum bitcointalk.org.
Running this requires Python and the libraries BeautifulSoup, Gensim, and SQLite3.

In order to download threads from the forums, run:

python download.py [database_path] [board_id]

In order to run topic modelling and export threads, run:

python modelTopics.py [database_path] [num_topics]

This will create a folder called "topics", with subfolders for each created topic.
Each of these will contain html files for those threads with the highest match with the given topic.
