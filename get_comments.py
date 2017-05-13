import requests
import os
import praw
from pprint import pprint
import datetime
from dotenv import find_dotenv, load_dotenv
from elasticsearch import Elasticsearch

# Setup praw
load_dotenv(find_dotenv())
CLIENT_SECRET = os.environ.get("SECRET")
CLIENT_ID = os.environ.get("CLIENT_ID")
USER_AGENT = os.environ.get("USER_aGENT")

reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)
print(reddit.read_only)


# Setup ES
es = Elasticsearch()
if es.indices.exists("rjobs"):
    es.indices.delete(index="rjobs")
res = es.indices.create(index="rjobs")

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

num_comments = 0
subreddit = 'jobs'
for submission in reddit.subreddit(subreddit).top(limit=None):

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
		res = es.index(index="rjobs", doc_type='comment', id=num_comments, body=body)
		num_comments += 1
		print(comment.submission.title)
		print(res['created'],num_comments)
	
