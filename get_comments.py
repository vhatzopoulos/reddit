import hashlib
import argparse
import os
import praw
from pprint import pprint
import datetime
from dotenv import find_dotenv, load_dotenv
from elasticsearch import Elasticsearch

# extract credentials from .env file
load_dotenv(find_dotenv())
CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
CLIENT_ID = os.environ.get("CLIENT_ID")
USER_AGENT = os.environ.get("USER_AGENT")


# extact index and subreddit from command line arguments
parser = argparse.ArgumentParser(description='Python program to collect comments from a subreddit and insert into Elasticsearch index.')

parser.add_argument('--subreddit', help='The subreddit to query', type=str)
parser.add_argument('--index', help='The elasticsearch index to insert the comments. Will be created if does not already exist', type=str)
args = parser.parse_args()
index, subreddit = args.index, args.subreddit


# Setup praw
reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)
print(reddit.read_only)

# Setup ES
es = Elasticsearch()
if es.indices.exists(index):
    es.indices.delete(index=index)
res = es.indices.create(index=index)

es.indices.put_mapping(
	index="rjobs",
	doc_type="comment",
	body={
		"properties": {
			"submission": {
				"type": "text"
				},
			"body": {
				"type": "text"
				},
			"score": {
				"type": "integer"
				},
			"ups":{
				"type": "integer"
				},
			"downs":{
				"type": "integer"
				},
			"gilded": {
				"type": "text"
				},
			"created": {
				"type": "date",
				"format" : "yyyy-MM-dd"
			}
		}
	}
)

#num_comments = 0

# loop through all submissions and all comments within them
for submission in reddit.subreddit(subreddit).top(limit=None):
	
	# take care of nested comments 
	submission.comments.replace_more(limit=0)
	for comment in submission.comments.list():
		dt = datetime.datetime.fromtimestamp(comment.created).strftime('%Y-%m-%d')
		
		body = {
			'submission':comment.submission.title,
			'body': comment.body,
			'score': comment.score,
			'ups': comment.ups,
			'downs': comment.downs,
			'created':dt,
			'gilded':comment.gilded
			}
		doc_id = hashlib.md5(body['body'].encode('utf-8')).hexdigest()
		res = es.index(index=index, doc_type='comment', id=doc_id, body=body)
		#num_comments += 1
		#print(comment.submission.title)
		#print(res['created'],num_comments)
		print(comment.submission.title, res['created'])
	
